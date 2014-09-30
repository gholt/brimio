package brimutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
)

// ChecksummedReader reads content written by ChecksummedWriter, verifying
// checksums when requested. Implements the io.ReadSeeker and io.Closer
// interfaces.
//
// Any errors from Read or Verify should make no assumptions about any
// resulting position and should Seek before continuing to use the
// ChecksummedReader.
type ChecksummedReader interface {
	// Read implements the io.Reader interface.
	//
	// Any error should make no assumption about any resulting position and
	// should Seek before continuing to use the ChecksummedReader.
	Read(v []byte) (n int, err error)
	// Seek implements the io.Seeker interface.
	Seek(offset int64, whence int) (n int64, err error)
	// Verify verifies the checksum for the section of the content containing
	// the current read position.
	//
	// If there is an error, whether the section is checksum valid is
	// indeterminate by this routine and the caller should decide what to do
	// based on the error. For example, if the underlying i/o causes a timeout
	// error, the content may be fine and just temporarily unreachable.
	//
	// Any error should also make no assumption about any resulting position
	// and should Seek before continuing to use the ChecksummedReader.
	//
	// With no error, the bool indicates whether the content is checksum valid
	// and the position within the ChecksummedReader will not have changed.
	Verify() (bool, error)
	// Close implements the io.Closer interface.
	Close() error
}

// NewChecksummedReader returns a ChecksummedReader that delegates requests to
// an underlying io.ReadSeeker expecting checksums of the content at given
// intervals using the hashing function given.
func NewChecksummedReader(delegate io.ReadSeeker, interval int, newHash func() hash.Hash32) ChecksummedReader {
	return newChecksummedReaderImpl(delegate, interval, newHash)
}

// ChecksummedWriter writes content with additional checksums embedded in the
// underlying content.
//
// Implements the io.WriteCloser interface.
//
// Note that this generally only works for brand new writers starting at offset
// 0. Appending to existing files or starting at offsets other than 0 requires
// special care when working with ChecksummedReader later and is beyond the
// basic usage described here.
//
// Also, do not forget to Close the ChecksummedWriter to ensure the final
// checksum is emitted.
type ChecksummedWriter interface {
	// Write implements the io.Writer interface.
	Write(v []byte) (n int, err error)
	// Close implements the io.Closer interface.
	Close() error
}

// NewChecksummedWriter returns a ChecksummedWriter that delegates requests to
// an underlying io.Writer and embeds checksums of the content at given
// intervals using the hashing function given.
func NewChecksummedWriter(delegate io.Writer, checksumInterval int, newHash func() hash.Hash32) ChecksummedWriter {
	return newChecksummedWriterImpl(delegate, checksumInterval, newHash)
}

type checksummedReaderImpl struct {
	delegate         io.ReadSeeker
	checksumInterval int
	checksumOffset   int
	newHash          func() hash.Hash32
	hash             hash.Hash32
	checksum         []byte
}

func newChecksummedReaderImpl(delegate io.ReadSeeker, interval int, newHash func() hash.Hash32) ChecksummedReader {
	return &checksummedReaderImpl{
		delegate:         delegate,
		checksumInterval: interval,
		newHash:          newHash,
		checksum:         make([]byte, 4),
	}
}

func (cri *checksummedReaderImpl) Read(v []byte) (int, error) {
	if cri.checksumOffset+len(v) > cri.checksumInterval {
		v = v[:cri.checksumInterval-cri.checksumOffset]
	}
	n, err := cri.delegate.Read(v)
	cri.checksumOffset += n
	if err == io.EOF {
		n -= 4
		if n < 0 {
			n = 0
		}
	} else if err == nil {
		if cri.checksumOffset == cri.checksumInterval {
			n2, err := io.ReadFull(cri.delegate, cri.checksum)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				n -= 4 - n2
				err = io.EOF
			}
			cri.checksumOffset = 0
		} else {
			n2, err := io.ReadFull(cri.delegate, cri.checksum)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				n -= 4 - n2
				err = io.EOF
			} else {
				_, err = cri.delegate.Seek(-int64(n2), 1)
			}
		}
	}
	return n, err
}

func (cri *checksummedReaderImpl) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
	case 1:
		o, err := cri.delegate.Seek(0, 1)
		cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
		if err != nil {
			return o - (o / int64(cri.checksumInterval) * 4), err
		}
		offset = o - (o / int64(cri.checksumInterval) * 4) + offset
	case 2:
		o, err := cri.delegate.Seek(0, 2)
		cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
		if err != nil {
			return o - (o / int64(cri.checksumInterval) * 4), err
		}
		offset = o - (o / int64(cri.checksumInterval) * 4) + offset
	default:
		o, _ := cri.delegate.Seek(0, 1)
		return o, fmt.Errorf("invalid whence %d", whence)
	}
	o, err := cri.delegate.Seek(offset+(offset/int64(cri.checksumInterval)*4), 0)
	cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
	return o - (o / int64(cri.checksumInterval) * 4), err
}

func (cri *checksummedReaderImpl) Verify() (bool, error) {
	originalOffset, err := cri.delegate.Seek(0, 1)
	if err != nil {
		return false, err
	}
	if cri.checksumOffset > 0 {
		_, err = cri.delegate.Seek(-int64(cri.checksumOffset), 1)
		if err != nil {
			return false, err
		}
	}
	block := make([]byte, cri.checksumInterval+4)
	checksum := block[cri.checksumInterval:]
	n, err := io.ReadFull(cri.delegate, block)
	if err == io.ErrUnexpectedEOF {
		checksum = block[n-4 : n]
		block = block[:n-4]
	} else if err != nil {
		return false, err
	} else {
		block = block[:cri.checksumInterval]
	}
	hash := cri.newHash()
	hash.Write(block)
	verified := bytes.Equal(checksum, hash.Sum(cri.checksum[:0]))
	_, err = cri.delegate.Seek(originalOffset, 0)
	if err != nil {
		return verified, err
	}
	return verified, nil
}

func (cri *checksummedReaderImpl) Close() error {
	var err error
	if c, ok := cri.delegate.(io.Closer); ok {
		err = c.Close()
	}
	cri.delegate = _ERR_DELEGATE
	return err
}

type checksummedWriterImpl struct {
	delegate         io.Writer
	checksumInterval int
	checksumOffset   int
	newHash          func() hash.Hash32
	hash             hash.Hash32
	checksum         []byte
}

func newChecksummedWriterImpl(delegate io.Writer, checksumInterval int, newHash func() hash.Hash32) *checksummedWriterImpl {
	return &checksummedWriterImpl{
		delegate:         delegate,
		checksumInterval: checksumInterval,
		newHash:          newHash,
		hash:             newHash(),
		checksum:         make([]byte, 4),
	}
}

func (cwi *checksummedWriterImpl) Write(v []byte) (int, error) {
	var n int
	var n2 int
	var err error
	for cwi.checksumOffset+len(v) >= cwi.checksumInterval {
		n2, err = cwi.delegate.Write(v[:cwi.checksumInterval-cwi.checksumOffset])
		n += n2
		if err != nil {
			cwi.delegate = _ERR_DELEGATE
			return n, err
		}
		cwi.hash.Write(v[:cwi.checksumInterval-cwi.checksumOffset])
		v = v[cwi.checksumInterval-cwi.checksumOffset:]
		binary.BigEndian.PutUint32(cwi.checksum, cwi.hash.Sum32())
		_, err = cwi.delegate.Write(cwi.checksum)
		if err != nil {
			cwi.delegate = _ERR_DELEGATE
			return n, err
		}
		cwi.hash = cwi.newHash()
		cwi.checksumOffset = 0
	}
	if len(v) > 0 {
		n2, err = cwi.delegate.Write(v)
		n += n2
		if err != nil {
			cwi.delegate = _ERR_DELEGATE
			return n, err
		}
		cwi.hash.Write(v)
		cwi.checksumOffset += n2
	}
	return n, err
}

func (cwi *checksummedWriterImpl) Close() error {
	var err error
	if cwi.checksumOffset > 0 {
		binary.BigEndian.PutUint32(cwi.checksum, cwi.hash.Sum32())
		_, err = cwi.delegate.Write(cwi.checksum)
		if err != nil {
			cwi.delegate = _ERR_DELEGATE
			return err
		}
	}
	if c, ok := cwi.delegate.(io.Closer); ok {
		err = c.Close()
	}
	cwi.delegate = _ERR_DELEGATE
	return err
}

type errDelegate struct {
}

var _ERR_DELEGATE = &errDelegate{}

func (ed *errDelegate) Read(v []byte) (int, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegate) Write(v []byte) (int, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegate) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegate) Close() error {
	return fmt.Errorf("already closed")
}
