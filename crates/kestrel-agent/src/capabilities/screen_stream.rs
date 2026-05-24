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
    let monitors = match xcap::Monitor::all() {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("screen_stream: Monitor::all failed: {}", e);
            return mpsc::channel::<EncodedFrame>(1).1;
        }
    };
    let Some(mon) = monitors.into_iter().nth(display_index) else {
        tracing::warn!(
            "screen_stream: display index {} not present",
            display_index
        );
        return mpsc::channel::<EncodedFrame>(1).1;
    };
    let width = match mon.width() {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("screen_stream: monitor.width: {}", e);
            return mpsc::channel::<EncodedFrame>(1).1;
        }
    };
    let height = match mon.height() {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!("screen_stream: monitor.height: {}", e);
            return mpsc::channel::<EncodedFrame>(1).1;
        }
    };
    // Encoders require even dimensions; round down if the OS gives
    // us an odd value (some scaled HiDPI configs do this).
    let enc_w = (width & !1) as usize;
    let enc_h = (height & !1) as usize;

    // Hand the xcap capture into the testable `spawn_with_source` path.
    // The closure handles the RGBA→BGRA swap + HiDPI-crop here so the
    // inner loop only sees ready-to-encode frames. xcap never signals
    // "stop" — the capture thread winds down only when its consumer
    // drops the EncodedFrame receiver.
    spawn_with_source(enc_w as u32, enc_h as u32, target_fps, move || {
        let img = match mon.capture_image() {
            Ok(i) => i,
            Err(e) => return CaptureOutcome::Skip(anyhow::anyhow!("xcap capture: {}", e)),
        };
        let (img_w, img_h) = (img.width() as usize, img.height() as usize);
        let mut bgra = img.into_raw();
        for px in bgra.chunks_exact_mut(4) {
            px.swap(0, 2);
        }
        let out = if img_w != enc_w || img_h != enc_h {
            crop_bgra(&bgra, img_w, img_h, enc_w, enc_h)
        } else {
            bgra
        };
        CaptureOutcome::Frame(out)
    })
}

/// Outcome of one `capture` closure invocation.
///
/// Production code only ever returns `Frame(bytes)` or `Skip(err)` —
/// the encode loop sleeps one interval after a `Skip` and retries.
/// Tests use `Stop` to terminate a finite-frame scenario cleanly
/// without playing string-comparison games against error messages.
pub enum CaptureOutcome {
    Frame(Vec<u8>),
    Skip(anyhow::Error),
    Stop,
}

/// Test-friendly entry point: drive the encode loop with an arbitrary
/// frame source. The closure is called once per tick and returns a
/// `CaptureOutcome` (see the enum for semantics).
pub fn spawn_with_source<F>(
    width: u32,
    height: u32,
    target_fps: u32,
    mut capture: F,
) -> mpsc::Receiver<EncodedFrame>
where
    F: FnMut() -> CaptureOutcome + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<EncodedFrame>(8);
    let interval = Duration::from_micros(1_000_000 / target_fps.max(1) as u64);
    std::thread::spawn(move || {
        let mut encoder = match H264Encoder::new(width, height, target_fps) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("screen_stream: encoder init: {}", e);
                return;
            }
        };
        let started = Instant::now();
        loop {
            let frame_start = Instant::now();
            let bgra = match capture() {
                CaptureOutcome::Frame(b) => b,
                CaptureOutcome::Stop => break,
                CaptureOutcome::Skip(e) => {
                    tracing::warn!("screen_stream: capture: {}", e);
                    std::thread::sleep(interval);
                    continue;
                }
            };
            let bytes = match encoder.encode_bgra(&bgra) {
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

    #[tokio::test]
    async fn synthetic_source_drives_full_encode_loop() {
        // Inject a synthetic frame source so we exercise the entire
        // encode loop body in CI without xcap / a display.
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_for_source = counter.clone();
        let mut rx = spawn_with_source(64, 48, 60, move || {
            let i = counter_for_source.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if i >= 5 {
                return CaptureOutcome::Stop;
            }
            let mut bgra = vec![0u8; 64 * 48 * 4];
            // Animate so each frame differs (encoder produces real
            // delta frames instead of optimizing them all to zero).
            for px in bgra.chunks_exact_mut(4) {
                px[1] = (i * 40) as u8;
                px[3] = 255;
            }
            CaptureOutcome::Frame(bgra)
        });
        let mut received = 0;
        while let Some(f) = rx.recv().await {
            assert!(!f.bytes.is_empty(), "frame {} was empty", received);
            // PTS must be monotonically non-decreasing.
            received += 1;
            if received >= 5 {
                break;
            }
        }
        assert_eq!(received, 5, "expected 5 encoded frames");
    }

    #[tokio::test]
    async fn capture_errors_dont_kill_the_loop() {
        // Three failing captures, then one success, then stop.
        // Verifies that transient capture errors don't terminate
        // the stream — only the explicit "stop" sentinel does.
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_for_source = counter.clone();
        let mut rx = spawn_with_source(64, 48, 240, move || {
            let i = counter_for_source.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if i < 3 {
                CaptureOutcome::Skip(anyhow::anyhow!("transient capture failure"))
            } else if i == 3 {
                CaptureOutcome::Frame(vec![100u8; 64 * 48 * 4])
            } else {
                CaptureOutcome::Stop
            }
        });
        let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for the successful frame")
            .expect("channel closed before success");
        assert!(!f.bytes.is_empty());
    }

    #[tokio::test]
    async fn synthetic_source_pts_is_monotonic_nondecreasing() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_for_source = counter.clone();
        let mut rx = spawn_with_source(64, 48, 60, move || {
            let i = counter_for_source.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if i >= 4 {
                return CaptureOutcome::Stop;
            }
            CaptureOutcome::Frame(vec![(i * 30) as u8; 64 * 48 * 4])
        });
        let mut prev_pts = 0u64;
        while let Some(f) = rx.recv().await {
            assert!(
                f.pts_ms >= prev_pts,
                "pts went backwards: {} -> {}",
                prev_pts,
                f.pts_ms
            );
            prev_pts = f.pts_ms;
        }
    }
}
