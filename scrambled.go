package brimutil

import (
	"math/rand"
	"time"
)

type Scrambled struct {
	r rand.Source
}

func NewScrambled() *Scrambled {
	return NewSeededScrambled(time.Now().UnixNano())
}

func NewSeededScrambled(seed int64) *Scrambled {
	return &Scrambled{r: rand.NewSource(seed)}
}

func (s *Scrambled) Read(bs []byte) {
	for i := len(bs) - 1; i >= 0; {
		v := s.r.Int63()
		for j := 7; i >= 0 && j >= 0; j-- {
			bs[i] = byte(v)
			i--
			v >>= 8
		}
	}
}
