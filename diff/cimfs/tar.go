package cimfs

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

const maxNanoSecondIntSize = 9

func parsePAXTime(t string) (time.Time, error) {
	buf := []byte(t)
	pos := bytes.IndexByte(buf, '.')
	var seconds, nanoseconds int64
	var err error
	if pos == -1 {
		seconds, err = strconv.ParseInt(t, 10, 0)
		if err != nil {
			return time.Time{}, err
		}
	} else {
		seconds, err = strconv.ParseInt(string(buf[:pos]), 10, 0)
		if err != nil {
			return time.Time{}, err
		}
		nano_buf := string(buf[pos+1:])
		// Pad as needed before converting to a decimal.
		// For example .030 -> .030000000 -> 30000000 nanoseconds
		if len(nano_buf) < maxNanoSecondIntSize {
			// Right pad
			nano_buf += strings.Repeat("0", maxNanoSecondIntSize-len(nano_buf))
		} else if len(nano_buf) > maxNanoSecondIntSize {
			// Right truncate
			nano_buf = nano_buf[:maxNanoSecondIntSize]
		}
		nanoseconds, err = strconv.ParseInt(string(nano_buf), 10, 0)
		if err != nil {
			return time.Time{}, err
		}
	}
	ts := time.Unix(seconds, nanoseconds)
	return ts, nil
}

func timeToFiletime(t time.Time) windows.Filetime {
	return windows.NsecToFiletime(t.UnixNano())
}

func fileInfoFromTar(h *tar.Header) (_ *cimfs.FileInfo, err error) {
	creationTime := time.Unix(0, 0)
	if creationTimeStr, ok := h.PAXRecords["LIBARCHIVE.creationtime"]; ok {
		creationTime, err = parsePAXTime(creationTimeStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse creation time")
		}
	}

	attrs := uint64(0)
	if attrsStr, ok := h.PAXRecords["MSWINDOWS.fileattr"]; ok {
		attrs, err = strconv.ParseUint(attrsStr, 10, 32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse attributes")
		}
	} else {
		if h.Typeflag == tar.TypeDir {
			attrs |= windows.FILE_ATTRIBUTE_DIRECTORY
		}
	}
	attrs &= ^uint64(windows.FILE_ATTRIBUTE_REPARSE_POINT)

	sd := []byte{}
	if sdStr, ok := h.PAXRecords["MSWINDOWS.rawsd"]; ok {
		sd, err = base64.StdEncoding.DecodeString(sdStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse security descriptor %q", sdStr)
		}
	}

	xattrPrefix := "MSWINDOWS.xattr."
	var eas []winio.ExtendedAttribute
	for k, v := range h.PAXRecords {
		if !strings.HasPrefix(k, xattrPrefix) {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode content for EA %s (%s)", k, v)
		}
		eas = append(eas, winio.ExtendedAttribute{
			Name:  k[len(xattrPrefix):],
			Value: data,
		})
	}

	return &cimfs.FileInfo{
		Size:               h.Size,
		CreationTime:       timeToFiletime(creationTime),
		LastWriteTime:      timeToFiletime(h.ModTime),
		ChangeTime:         timeToFiletime(h.ChangeTime),
		LastAccessTime:     timeToFiletime(h.AccessTime),
		Attributes:         uint32(attrs),
		SecurityDescriptor: sd,
		EAs:                eas,
	}, nil
}
