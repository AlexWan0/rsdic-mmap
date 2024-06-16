package rsdic

import (
	"encoding/binary"
	"fmt"
	"io"
)

func floor(num uint64, div uint64) uint64 {
	return (num + div - 1) / div
}

func decompose(x uint64, y uint64) (uint64, uint64) {
	return x / y, x % y
}

func setSlice(bits []uint64, pos uint64, codeLen uint8, val uint64) {
	if codeLen == 0 {
		return
	}
	block, offset := decompose(pos, kSmallBlockSize)
	bits[block] |= val << offset
	if offset+uint64(codeLen) > kSmallBlockSize {
		bits[block+1] |= (val >> (kSmallBlockSize - offset))
	}
}

func setSliceBuffer(bits *BufferedBits, pos uint64, codeLen uint8, val uint64) {
	bitsBuffer := bits.writeBits
	isSet := bits.isSet
	bufferSize := bits.writeBitsSize

	// fmt.Println(bits)

	if codeLen == 0 {
		return
	}
	block, offset := decompose(pos, kSmallBlockSize)
	bitsBuffer[block-(bufferSize-2)] |= val << offset
	isSet[block-(bufferSize-2)] = true

	// bits[block] |= val << offset

	// if int64(block) < int64(len(bits)-2) {
	// 	panic(fmt.Sprintf("block: %d, len(bits): %d", block, len(bits)))
	// }

	if offset+uint64(codeLen) > kSmallBlockSize {
		bitsBuffer[block-(bufferSize-2)+1] |= (val >> (kSmallBlockSize - offset))
		isSet[block-(bufferSize-2)+1] = true

		// bits[block+1] |= (val >> (kSmallBlockSize - offset))
	}
}

func getBit(x uint64, pos uint8) bool {
	return ((x >> pos) & 1) == 1
}

func getChunk(r io.ReaderAt, bits *BufferedBits, pos uint64) uint64 {
	numOnDisk := bits.numWritten

	if pos < numOnDisk {
		// fmt.Println("retrieving from disk", pos)
		toRead := make([]byte, 8)
		n, err := r.ReadAt(toRead, int64(pos*8))

		if n < 8 {
			panic("getChunk: not enough bytes read")
		}

		if err != nil {
			panic(err) // TODO: handle
		}

		// fmt.Println(pos, toRead)

		readChunk := binary.LittleEndian.Uint64(toRead)

		return readChunk
	} else if bits.isSet[pos-numOnDisk] {
		// fmt.Println("retrieving from buffer")
		return bits.writeBits[pos-numOnDisk]
	}

	panic("getChunk: out of bounds")
}

func getSlice(bits []uint64, pos uint64, codeLen uint8) uint64 {
	if codeLen == 0 {
		return 0
	}
	block, offset := decompose(pos, kSmallBlockSize)
	ret := (bits[block] >> offset)
	if offset+uint64(codeLen) > kSmallBlockSize {
		ret |= (bits[block+1] << (kSmallBlockSize - offset))
	}
	if codeLen == 64 {
		return ret
	}
	return ret & ((1 << codeLen) - 1)
}

func getSliceBuffer(r io.ReaderAt, bits *BufferedBits, pos uint64, codeLen uint8) uint64 {
	if codeLen == 0 {
		return 0
	}
	block, offset := decompose(pos, kSmallBlockSize)
	ret := (getChunk(r, bits, block) >> offset)
	if offset+uint64(codeLen) > kSmallBlockSize {
		ret |= (getChunk(r, bits, block+1) << (kSmallBlockSize - offset))
	}
	if codeLen == 64 {
		return ret
	}
	return ret & ((1 << codeLen) - 1)
}

func bitNum(x uint64, n uint64, b bool) uint64 {
	if b {
		return x
	} else {
		return n - x
	}
}

func printBit(x uint64) {
	for i := 0; i < 64; i++ {
		fmt.Printf("%d", i%10)
	}
	fmt.Printf("\n")
	for i := uint8(0); i < 64; i++ {
		if getBit(x, i) {
			fmt.Printf("1")
		} else {
			fmt.Printf("0")
		}
	}
	fmt.Printf("\n")
}

func popCount(x uint64) uint8 {
	x = x - ((x & 0xAAAAAAAAAAAAAAAA) >> 1)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0F
	return uint8(x * 0x0101010101010101 >> 56)
}
