//go:build cgo

package meowcaller

/*
#cgo pkg-config: opus
#include <opus.h>
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

type opusFrameEncoder struct {
	mu  sync.Mutex
	ptr *C.OpusEncoder
}
type opusFrameDecoder struct {
	mu  sync.Mutex
	ptr *C.OpusDecoder
}

func opusCodecAvailable() bool { return true }

func newOpusFrameEncoder() (audioFrameEncoder, error) {
	var status C.int
	ptr := C.opus_encoder_create(C.opus_int32(SampleRate), 1, C.OPUS_APPLICATION_VOIP, &status)
	if status != C.OPUS_OK || ptr == nil {
		return nil, fmt.Errorf("meowcaller: create Opus encoder: %s", C.GoString(C.opus_strerror(status)))
	}
	return &opusFrameEncoder{ptr: ptr}, nil
}

func newOpusFrameDecoder() (participantAudioDecoder, error) {
	var status C.int
	ptr := C.opus_decoder_create(C.opus_int32(SampleRate), 1, &status)
	if status != C.OPUS_OK || ptr == nil {
		return nil, fmt.Errorf("meowcaller: create Opus decoder: %s", C.GoString(C.opus_strerror(status)))
	}
	return &opusFrameDecoder{ptr: ptr}, nil
}

func (e *opusFrameEncoder) Encode(pcm []float32) ([]byte, error) {
	if e == nil || len(pcm) != FrameSamples {
		return nil, fmt.Errorf("meowcaller: Opus encoder requires one %d-sample frame", FrameSamples)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ptr == nil {
		return nil, fmt.Errorf("meowcaller: Opus encoder is closed")
	}
	output := make([]byte, 4000)
	n := C.opus_encode_float(e.ptr, (*C.float)(unsafe.Pointer(&pcm[0])), C.int(len(pcm)), (*C.uchar)(unsafe.Pointer(&output[0])), C.opus_int32(len(output)))
	if n < 0 {
		return nil, fmt.Errorf("meowcaller: encode Opus: %s", C.GoString(C.opus_strerror(n)))
	}
	return output[:n], nil
}

func (e *opusFrameEncoder) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ptr != nil {
		C.opus_encoder_destroy(e.ptr)
		e.ptr = nil
	}
	return nil
}

func (d *opusFrameDecoder) Decode(packet []byte) []float32 {
	if d == nil || len(packet) == 0 {
		return make([]float32, FrameSamples)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ptr == nil {
		return make([]float32, FrameSamples)
	}
	pcm := make([]float32, 5760)
	n := C.opus_decode_float(d.ptr, (*C.uchar)(unsafe.Pointer(&packet[0])), C.opus_int32(len(packet)), (*C.float)(unsafe.Pointer(&pcm[0])), C.int(len(pcm)), 0)
	if n < 0 {
		return make([]float32, FrameSamples)
	}
	return pcm[:n]
}

func (d *opusFrameDecoder) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ptr != nil {
		C.opus_decoder_destroy(d.ptr)
		d.ptr = nil
	}
	return nil
}
