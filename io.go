package rsdic

import (
	"io"
	"os"

	"golang.org/x/exp/mmap"
)

type Readers struct {
	bitsReader io.ReaderAt
}

type Writers struct {
	bitsWriter io.Writer
}

func InitReaders(bitsPath string) (*Readers, error) {
	bitsReader, err := mmap.Open(bitsPath)
	if err != nil {
		return nil, err
	}

	return &Readers{
		bitsReader: bitsReader,
	}, nil
}

func InitWriters(bitsPath string) (*Writers, error) {
	writer, err := os.Create(bitsPath)
	if err != nil {
		return nil, err
	}

	return &Writers{
		bitsWriter: writer,
	}, nil
}

func (w *Writers) Close() error {
	if closer, ok := w.bitsWriter.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
