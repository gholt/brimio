// Copyright 2014 Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package brimutil

import (
	"testing"
)

func TestNormalizePath(t *testing.T) {
	out := NormalizePath("a")
	exp := "a"
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	out = NormalizePath("a/..")
	exp = "."
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	out = NormalizePath("a/../b")
	exp = "b"
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	out = NormalizePath("a/../../b")
	exp = "../b"
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
}
