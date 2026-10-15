//go:build cgo

package call_service

/*
#cgo pkg-config: opus
#include <opus.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type callAudioEncoder interface {
	Encode([]float32, []byte) (int, error)
	Close() error
}

type callAudioDecoder interface {
	Decode([]byte, []float32) (int, error)
	Close() error
}

type opusEncoder struct{ ptr *C.OpusEncoder }
type opusDecoder struct{ ptr *C.OpusDecoder }

func newCallAudioEncoder() (callAudioEncoder, error) {
	return newOpusEncoder(callAudioRate)
}

func newOpusEncoder(sampleRate int) (*opusEncoder, error) {
	var status C.int
	ptr := C.opus_encoder_create(C.opus_int32(sampleRate), 2, C.OPUS_APPLICATION_VOIP, &status)
	if status != C.OPUS_OK || ptr == nil {
		return nil, fmt.Errorf("create opus encoder: %s", C.GoString(C.opus_strerror(status)))
	}
	return &opusEncoder{ptr: ptr}, nil
}

func newCallAudioDecoder() (callAudioDecoder, error) {
	var status C.int
	ptr := C.opus_decoder_create(48000, 2, &status)
	if status != C.OPUS_OK || ptr == nil {
		return nil, fmt.Errorf("create opus decoder: %s", C.GoString(C.opus_strerror(status)))
	}
	return &opusDecoder{ptr: ptr}, nil
}

func (e *opusEncoder) Encode(pcm []float32, output []byte) (int, error) {
	if e == nil || e.ptr == nil || len(pcm) == 0 || len(output) == 0 {
		return 0, fmt.Errorf("opus encoder is closed or buffer is empty")
	}
	if len(pcm)%2 != 0 {
		return 0, fmt.Errorf("stereo opus input must contain pairs of samples")
	}
	n := C.opus_encode_float(e.ptr, (*C.float)(unsafe.Pointer(&pcm[0])), C.int(len(pcm)/2), (*C.uchar)(unsafe.Pointer(&output[0])), C.opus_int32(len(output)))
	if n < 0 {
		return 0, fmt.Errorf("encode opus: %s", C.GoString(C.opus_strerror(n)))
	}
	return int(n), nil
}

func (d *opusDecoder) Decode(packet []byte, pcm []float32) (int, error) {
	if d == nil || d.ptr == nil || len(pcm) == 0 {
		return 0, fmt.Errorf("opus decoder is closed or output buffer is empty")
	}
	if len(pcm)%2 != 0 {
		return 0, fmt.Errorf("stereo opus output buffer must contain pairs of samples")
	}
	var packetPtr *C.uchar
	if len(packet) > 0 {
		packetPtr = (*C.uchar)(unsafe.Pointer(&packet[0]))
	}
	// Opus treats a nil packet with a zero length as a packet-loss-concealment
	// request, preserving decoder state while producing the missing PCM frame.
	n := C.opus_decode_float(d.ptr, packetPtr, C.opus_int32(len(packet)), (*C.float)(unsafe.Pointer(&pcm[0])), C.int(len(pcm)/2), 0)
	if n < 0 {
		return 0, fmt.Errorf("decode opus: %s", C.GoString(C.opus_strerror(n)))
	}
	return int(n), nil
}

func (d *opusDecoder) DecodePLC(samples int, pcm []float32) (int, error) {
	if d == nil || d.ptr == nil || samples <= 0 || len(pcm) < samples*2 {
		return 0, fmt.Errorf("opus decoder is closed or PLC output buffer is too small")
	}
	n := C.opus_decode_float(d.ptr, nil, 0, (*C.float)(unsafe.Pointer(&pcm[0])), C.int(samples), 0)
	if n < 0 {
		return 0, fmt.Errorf("conceal opus packet loss: %s", C.GoString(C.opus_strerror(n)))
	}
	return int(n), nil
}

func (e *opusEncoder) Close() error {
	if e != nil && e.ptr != nil {
		C.opus_encoder_destroy(e.ptr)
		e.ptr = nil
	}
	return nil
}

func (d *opusDecoder) Close() error {
	if d != nil && d.ptr != nil {
		C.opus_decoder_destroy(d.ptr)
		d.ptr = nil
	}
	return nil
}
