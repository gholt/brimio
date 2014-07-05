// Copyright 2014 Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package brimutil contains tools there isn't another place for yet.
package brimutil

import (
	"os/user"
	"path"
	"path/filepath"
	"strings"
)

// NormalizePath returns a path with any "." or ".." instances resolved
// (using path.Clean) and also attempts to resolve "~" and "~user" to home
// directories.
func NormalizePath(value string) string {
	value = path.Clean(filepath.ToSlash(value))
	if value[0] == '~' {
		if value[1] == '/' {
			if usr, err := user.Current(); err == nil {
				return strings.Replace(value, "~", usr.HomeDir, 1)
			}
		} else {
			parts := strings.SplitN(value, "/", 2)
			if usr, err := user.Lookup(parts[0][1:]); err == nil {
				return path.Join(usr.HomeDir, parts[1])
			}
		}
	}
	return value
}
