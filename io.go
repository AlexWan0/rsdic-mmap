package rsdic

import (
	"encoding/binary"
	"io"
	"os"
	"path"

	"golang.org/x/exp/mmap"
)

const (
	BITS_FN             = "bits.bin"
	POINTER_BLOCK_FN    = "pointer.bin"
	RANK_BLOCK_FN       = "rank_block.bin"
	SELECT_ONE_IND_FN   = "select_one_ind.bin"
	SELECT_ZERO_IND_FN  = "select_zero_ind.bin"
	RANK_SMALL_BLOCK_FN = "rank_small_block.bin"
)

type Readers struct {
	bitsReader       io.ReaderAt
	pointerReader    io.ReaderAt
	rankReader       io.ReaderAt
	selectOneReader  io.ReaderAt
	selectZeroReader io.ReaderAt
	rankSmallReader  io.ReaderAt
}

type Writers struct {
	bitsWriter       io.Writer
	pointerWriter    io.Writer
	rankWriter       io.Writer
	selectOneWriter  io.Writer
	selectZeroWriter io.Writer
	rankSmallWriter  io.Writer
}

func InitReaders(bitsPath string) (*Readers, error) {
	bitsReader, err := mmap.Open(path.Join(bitsPath, BITS_FN))
	if err != nil {
		return nil, err
	}

	pointerReader, err := mmap.Open(path.Join(bitsPath, POINTER_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	rankReader, err := mmap.Open(path.Join(bitsPath, RANK_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	selectOneReader, err := mmap.Open(path.Join(bitsPath, SELECT_ONE_IND_FN))
	if err != nil {
		return nil, err
	}

	selectZeroReader, err := mmap.Open(path.Join(bitsPath, SELECT_ZERO_IND_FN))
	if err != nil {
		return nil, err
	}

	rankSmallReader, err := mmap.Open(path.Join(bitsPath, RANK_SMALL_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	return &Readers{
		bitsReader:       bitsReader,
		pointerReader:    pointerReader,
		rankReader:       rankReader,
		selectOneReader:  selectOneReader,
		selectZeroReader: selectZeroReader,
		rankSmallReader:  rankSmallReader,
	}, nil
}

func InitWriters(bitsPath string) (*Writers, error) {
	writer, err := os.Create(path.Join(bitsPath, BITS_FN))
	if err != nil {
		return nil, err
	}

	pointerWriter, err := os.Create(path.Join(bitsPath, POINTER_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	rankWriter, err := os.Create(path.Join(bitsPath, RANK_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	selectOneWriter, err := os.Create(path.Join(bitsPath, SELECT_ONE_IND_FN))
	if err != nil {
		return nil, err
	}

	selectZeroWriter, err := os.Create(path.Join(bitsPath, SELECT_ZERO_IND_FN))
	if err != nil {
		return nil, err
	}

	rankSmallWriter, err := os.Create(path.Join(bitsPath, RANK_SMALL_BLOCK_FN))
	if err != nil {
		return nil, err
	}

	return &Writers{
		bitsWriter:       writer,
		pointerWriter:    pointerWriter,
		rankWriter:       rankWriter,
		selectOneWriter:  selectOneWriter,
		selectZeroWriter: selectZeroWriter,
		rankSmallWriter:  rankSmallWriter,
	}, nil
}

func (w *Writers) Close() error {
	if closer, ok := w.bitsWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.pointerWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.rankWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.selectOneWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.selectZeroWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.rankSmallWriter.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func appendUint64(w io.Writer, val uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)

	_, err := w.Write(buf)
	if err != nil {
		panic(err)
	}
}

func readUint64(r io.ReaderAt, pos uint64) uint64 {
	buf := make([]byte, 8)
	_, err := r.ReadAt(buf, int64(pos*8))
	if err != nil {
		panic(err)
	}

	result := binary.LittleEndian.Uint64(buf)
	// fmt.Println("readUint64", result)

	return result
}

func appendUint8(w io.Writer, val uint8) {
	buf := []uint8{val}

	_, err := w.Write(buf)
	if err != nil {
		panic(err)
	}
}

func readUint8(r io.ReaderAt, pos uint64) uint8 {
	buf := make([]byte, 1)
	_, err := r.ReadAt(buf, int64(pos))
	if err != nil {
		panic(err)
	}

	return buf[0]
}
