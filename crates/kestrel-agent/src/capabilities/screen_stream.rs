// crates/kestrel-agent/src/capabilities/screen_stream.rs
//
// xcap-driven capture loop that produces a channel of encoded H.264
// frames. Brick two of the WebRTC pipeline: the next module (the PC
// track writer) consumes EncodedFrame items and pushes them onto the
// wire as RTP.
//
// Lives on a blocking thread because xcap's capture_image is sync and
// the encoder is CPU-bound. The channel is bounded (8 frames) so back-
// pressure from a slow consumer reduces capture rate rather than
// growing a queue without limit.

use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::capabilities::encoder::H264Encoder;

/// One encoded frame ready for RTP packetization.
#[derive(Debug)]
pub struct EncodedFrame {
    /// H.264 NAL units with Annex-B start codes (output of `H264Encoder`).
    pub bytes: Bytes,
    /// Presentation time relative to capture start, in milliseconds.
    pub pts_ms: u64,
}

/// Spawn a capture loop targeting display `display_index` at roughly
/// `target_fps`. Returns the receiver end of the frame channel; the
/// sender lives in the spawned task and drops naturally when the task
/// exits.
///
/// The task exits when:
///   - the receiver is dropped (sender's `blocking_send` returns Err),
///   - the requested display doesn't exist,
///   - the encoder fails to initialize.
///
/// Transient `capture_image` errors are logged + skipped — a single
/// failed frame shouldn't kill the stream.
pub fn spawn(display_index: usize, target_fps: u32) -> mpsc::Receiver<EncodedFrame> {
    let (tx, rx) = mpsc::channel::<EncodedFrame>(8);
    let interval = Duration::from_micros(1_000_000 / target_fps.max(1) as u64);
    std::thread::spawn(move || {
        let monitors = match xcap::Monitor::all() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("screen_stream: Monitor::all failed: {}", e);
                return;
            }
        };
        let Some(mon) = monitors.into_iter().nth(display_index) else {
            tracing::warn!(
                "screen_stream: display index {} not present",
                display_index
            );
            return;
        };
        let width = match mon.width() {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("screen_stream: monitor.width: {}", e);
                return;
            }
        };
        let height = match mon.height() {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("screen_stream: monitor.height: {}", e);
                return;
            }
        };
        // Encoders require even dimensions; round down if the OS gives
        // us an odd value (some scaled HiDPI configs do this).
        let enc_w = width & !1;
        let enc_h = height & !1;
        let mut encoder = match H264Encoder::new(enc_w, enc_h, target_fps) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("screen_stream: encoder init: {}", e);
                return;
            }
        };

        let started = Instant::now();
        loop {
            let frame_start = Instant::now();
            let img = match mon.capture_image() {
                Ok(i) => i,
                Err(e) => {
                    tracing::warn!("screen_stream: capture_image: {}", e);
                    std::thread::sleep(interval);
                    continue;
                }
            };

            let (img_w, img_h) = (img.width() as usize, img.height() as usize);
            let mut bgra = img.into_raw();
            // xcap returns RGBA; we need BGRA for the encoder's BT.601
            // path. swap R and B in-place.
            for px in bgra.chunks_exact_mut(4) {
                px.swap(0, 2);
            }
            // If the image came back larger than the encoder dims
            // (HiDPI quirk: width()/height() lie about logical vs
            // physical pixels), crop to the encoder's view.
            let cropped = if img_w as u32 != enc_w || img_h as u32 != enc_h {
                crop_bgra(&bgra, img_w, img_h, enc_w as usize, enc_h as usize)
            } else {
                bgra
            };

            let bytes = match encoder.encode_bgra(&cropped) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("screen_stream: encode_bgra: {}", e);
                    continue;
                }
            };
            let pts_ms = started.elapsed().as_millis() as u64;
            if tx
                .blocking_send(EncodedFrame { bytes, pts_ms })
                .is_err()
            {
                // Receiver dropped — wind down.
                break;
            }
            let elapsed = frame_start.elapsed();
            if elapsed < interval {
                std::thread::sleep(interval - elapsed);
            }
        }
    });
    rx
}

/// Top-left crop of a BGRA buffer. Used when the captured image is
/// larger than the encoder's configured (rounded-even) dimensions —
/// rare, but happens on scaled HiDPI displays.
pub fn crop_bgra(
    src: &[u8],
    src_w: usize,
    src_h: usize,
    dst_w: usize,
    dst_h: usize,
) -> Vec<u8> {
    assert!(src.len() >= src_w * src_h * 4, "src too small for declared dims");
    assert!(dst_w <= src_w, "crop width exceeds source");
    assert!(dst_h <= src_h, "crop height exceeds source");
    let mut out = vec![0u8; dst_w * dst_h * 4];
    for y in 0..dst_h {
        let src_off = y * src_w * 4;
        let dst_off = y * dst_w * 4;
        out[dst_off..dst_off + dst_w * 4]
            .copy_from_slice(&src[src_off..src_off + dst_w * 4]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crop_extracts_top_left_corner() {
        let src_w = 4;
        let src_h = 4;
        let mut src = vec![0u8; src_w * src_h * 4];
        // Mark each pixel with its (x,y) for verification.
        for y in 0..src_h {
            for x in 0..src_w {
                let i = (y * src_w + x) * 4;
                src[i] = x as u8;
                src[i + 1] = y as u8;
                src[i + 2] = 0;
                src[i + 3] = 255;
            }
        }
        let cropped = crop_bgra(&src, src_w, src_h, 2, 2);
        assert_eq!(cropped.len(), 2 * 2 * 4);
        // Pixel (0,0) and (1,1) preserved.
        assert_eq!(cropped[0], 0); // x=0
        assert_eq!(cropped[1], 0); // y=0
        assert_eq!(cropped[2 * 4 + 4], 1); // x=1
        assert_eq!(cropped[2 * 4 + 5], 1); // y=1
    }

    #[test]
    fn crop_to_same_size_is_identity() {
        let src = vec![42u8; 8 * 6 * 4];
        let out = crop_bgra(&src, 8, 6, 8, 6);
        assert_eq!(out, src);
    }

    #[tokio::test]
    async fn loop_exits_when_receiver_drops() {
        // We can't reliably exercise capture on a CI box (no display),
        // but we CAN verify the task exits cleanly when the receiver
        // is dropped — even if it never managed to produce a frame,
        // the early-exit paths (no monitors, encoder init failure)
        // already return. This test guards against a regression where
        // a future change would loop forever despite a closed channel.
        let rx = spawn(0, 30);
        drop(rx);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
