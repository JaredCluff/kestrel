// crates/kestrel-agent/src/capabilities/encoder.rs
//
// H.264 encoder wrapper + BGRA→YUV420 colorspace conversion. First
// brick of the WebRTC streaming pipeline: a screen-capture task feeds
// BGRA frames in, encoded NAL units come out, the webrtc track writer
// downstream packetizes them into RTP.
//
// The conversion is naive (per-pixel, no SIMD). At 1920x1080 / 30fps
// that's ~62 MPix/s of work — measurable but not the bottleneck on
// modern hardware. SIMD acceleration is a follow-up if real-world
// profiling shows it dominates.

use bytes::{Bytes, BytesMut};

/// Thin wrapper around openh264's Encoder that owns the configured
/// dimensions and exposes a single BGRA-in, NAL-units-out method.
pub struct H264Encoder {
    inner: openh264::encoder::Encoder,
    width: u32,
    height: u32,
}

impl H264Encoder {
    pub fn new(width: u32, height: u32, target_fps: u32) -> anyhow::Result<Self> {
        anyhow::ensure!(width % 2 == 0, "width must be even (got {})", width);
        anyhow::ensure!(height % 2 == 0, "height must be even (got {})", height);

        let config = openh264::encoder::EncoderConfig::new()
            .max_frame_rate(target_fps as f32)
            .rate_control_mode(openh264::encoder::RateControlMode::Bitrate)
            .set_bitrate_bps(2_000_000)
            .enable_skip_frame(true);
        let inner = openh264::encoder::Encoder::with_api_config(
            openh264::OpenH264API::from_source(),
            config,
        )?;
        Ok(Self { inner, width, height })
    }

    /// Encode one BGRA frame. Returns the H.264 NAL units (with Annex-B
    /// start codes) concatenated into a single Bytes. For the very
    /// first frame this includes SPS + PPS + an IDR keyframe; later
    /// frames are P-frames with periodic IDR refreshes per the
    /// encoder's internal scheduling.
    pub fn encode_bgra(&mut self, bgra: &[u8]) -> anyhow::Result<Bytes> {
        let expected = (self.width as usize) * (self.height as usize) * 4;
        anyhow::ensure!(
            bgra.len() == expected,
            "BGRA buffer length {} != expected {} ({}x{}*4)",
            bgra.len(),
            expected,
            self.width,
            self.height,
        );
        let yuv_vec = bgra_to_yuv420(bgra, self.width as usize, self.height as usize);
        let yuv = openh264::formats::YUVBuffer::from_vec(
            yuv_vec,
            self.width as usize,
            self.height as usize,
        );
        let out = self.inner.encode(&yuv)?;

        let mut buf = BytesMut::new();
        for li in 0..out.num_layers() {
            let Some(layer) = out.layer(li) else { continue };
            for ni in 0..layer.nal_count() {
                if let Some(nal) = layer.nal_unit(ni) {
                    buf.extend_from_slice(nal);
                }
            }
        }
        Ok(buf.freeze())
    }
}

/// Naive BGRA→I420 (YUV 4:2:0 planar) conversion. Output layout is
/// `[Y plane | U plane | V plane]`, matching openh264's `YUVBuffer`
/// layout. BT.601 limited-range coefficients — same as what most
/// software encoders and web browsers expect by default.
pub fn bgra_to_yuv420(bgra: &[u8], width: usize, height: usize) -> Vec<u8> {
    assert_eq!(width % 2, 0, "width must be even");
    assert_eq!(height % 2, 0, "height must be even");
    assert_eq!(bgra.len(), width * height * 4, "BGRA buffer size mismatch");

    let y_size = width * height;
    let uv_size = (width / 2) * (height / 2);
    let mut out = vec![0u8; y_size + 2 * uv_size];
    let (y_plane, uv) = out.split_at_mut(y_size);
    let (u_plane, v_plane) = uv.split_at_mut(uv_size);

    let chroma_w = width / 2;
    for py in 0..height {
        for px in 0..width {
            let i = (py * width + px) * 4;
            let b = bgra[i] as f32;
            let g = bgra[i + 1] as f32;
            let r = bgra[i + 2] as f32;

            let yv = (0.257 * r + 0.504 * g + 0.098 * b + 16.0).round().clamp(0.0, 255.0);
            y_plane[py * width + px] = yv as u8;

            if px % 2 == 0 && py % 2 == 0 {
                let uv_val = (-0.148 * r - 0.291 * g + 0.439 * b + 128.0)
                    .round()
                    .clamp(0.0, 255.0);
                let vv = (0.439 * r - 0.368 * g - 0.071 * b + 128.0)
                    .round()
                    .clamp(0.0, 255.0);
                let ci = (py / 2) * chroma_w + (px / 2);
                u_plane[ci] = uv_val as u8;
                v_plane[ci] = vv as u8;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bgra_to_yuv420_produces_expected_size() {
        let bgra = vec![128u8; 64 * 48 * 4];
        let yuv = bgra_to_yuv420(&bgra, 64, 48);
        assert_eq!(yuv.len(), 64 * 48 + 32 * 24 * 2);
    }

    #[test]
    fn bgra_to_yuv420_pure_white_maps_to_full_luma() {
        // RGB(255,255,255) → Y = 235 (limited range), U=V=128.
        let bgra = vec![255u8; 4 * 4 * 4];
        let yuv = bgra_to_yuv420(&bgra, 4, 4);
        for &y in &yuv[..16] {
            assert!((230..=240).contains(&y), "Y for white should be ~235, got {}", y);
        }
        for &c in &yuv[16..] {
            assert!((126..=130).contains(&c), "chroma should be ~128, got {}", c);
        }
    }

    #[test]
    fn bgra_to_yuv420_pure_red_has_positive_v_offset() {
        // Red has strong V (red-difference) channel.
        let mut bgra = vec![0u8; 4 * 4 * 4];
        for px in bgra.chunks_exact_mut(4) {
            px[2] = 255; // R
        }
        let yuv = bgra_to_yuv420(&bgra, 4, 4);
        let v_start = 16 + 4;
        for &v in &yuv[v_start..v_start + 4] {
            assert!(v > 200, "V for red should be ≫ 128, got {}", v);
        }
    }

    #[test]
    fn encoder_constructor_rejects_odd_dimensions() {
        assert!(H264Encoder::new(63, 48, 30).is_err());
        assert!(H264Encoder::new(64, 47, 30).is_err());
    }

    #[test]
    fn encoder_produces_nonempty_bitstream_for_first_frame() {
        let mut enc = H264Encoder::new(64, 48, 30).unwrap();
        let bgra = vec![200u8; 64 * 48 * 4];
        let frame = enc.encode_bgra(&bgra).unwrap();
        assert!(
            !frame.is_empty(),
            "first encoded frame must produce SPS/PPS + IDR NAL units"
        );
        // First three bytes are the Annex-B start code 0x00 0x00 0x01
        // (or 0x00 0x00 0x00 0x01); verify we got SOMETHING that
        // starts with a start code prefix.
        assert!(
            frame.starts_with(&[0, 0, 0, 1]) || frame.starts_with(&[0, 0, 1]),
            "expected Annex-B start code prefix, got {:02x?}",
            &frame[..frame.len().min(8)]
        );
    }

    #[test]
    fn encoder_handles_multiple_frames_in_sequence() {
        let mut enc = H264Encoder::new(64, 48, 30).unwrap();
        let mut first_size = 0;
        for i in 0..5 {
            let mut bgra = vec![0u8; 64 * 48 * 4];
            // Vary content so the encoder produces a real delta.
            for px in bgra.chunks_exact_mut(4) {
                px[1] = (i * 50) as u8;
            }
            let frame = enc.encode_bgra(&bgra).unwrap();
            assert!(!frame.is_empty(), "frame {} was empty", i);
            if i == 0 {
                first_size = frame.len();
            } else {
                // Subsequent P-frames should generally be smaller than
                // the SPS+PPS+IDR opener. Loose check — just confirm
                // the encoder didn't blow up.
                assert!(frame.len() < first_size * 10, "frame {} grew unbounded", i);
            }
        }
    }

    #[test]
    fn encode_bgra_rejects_wrong_buffer_size() {
        let mut enc = H264Encoder::new(64, 48, 30).unwrap();
        let too_small = vec![0u8; 10];
        assert!(enc.encode_bgra(&too_small).is_err());
    }
}
