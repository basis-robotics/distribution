package chunked

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
)

// ConvertLayerToChunked decompresses a gzip or zstd tar layer and converts each file
// to a chunk blob. blobPusher is called once per file with its uncompressed content;
// it must store the blob and return its digest. Empty files produce a TOC entry with
// no digest (no blob pushed). Returns the completed TOC.
func ConvertLayerToChunked(ctx context.Context, compressedData []byte, mediaType string,
	blobPusher func(ctx context.Context, data []byte) (digest.Digest, error)) (*TOC, error) {

	tarReader, err := decompressLayer(compressedData, mediaType)
	if err != nil {
		return nil, fmt.Errorf("chunked: decompressing layer: %w", err)
	}

	toc := &TOC{Version: 1}
	tr := tar.NewReader(tarReader)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("chunked: reading tar: %w", err)
		}

		entry, err := convertTarHeader(ctx, hdr, tr, blobPusher)
		if err != nil {
			return nil, err
		}
		toc.Entries = append(toc.Entries, entry)
	}

	return toc, nil
}

// decompressLayer returns an io.Reader over the uncompressed tar stream.
func decompressLayer(data []byte, mediaType string) (io.Reader, error) {
	r := bytes.NewReader(data)

	switch {
	case strings.Contains(mediaType, "gzip"):
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return gr, nil

	case strings.Contains(mediaType, "zstd"):
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil

	default:
		// Assume uncompressed tar.
		return r, nil
	}
}

// convertTarHeader converts a single tar header and its content into a TOCEntry.
func convertTarHeader(ctx context.Context, hdr *tar.Header, tr io.Reader,
	blobPusher func(ctx context.Context, data []byte) (digest.Digest, error)) (TOCEntry, error) {

	name := hdr.Name

	// Determine the base file name and directory prefix for whiteout handling.
	base := name
	dir := ""
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		dir = name[:idx+1]
		base = name[idx+1:]
	}

	// Opaque whiteout: entire directory is replaced.
	if base == ".wh..wh..opq" {
		return TOCEntry{
			Type: "opaque",
			Name: dir,
		}, nil
	}

	// Regular whiteout: a specific file is deleted.
	if strings.HasPrefix(base, ".wh.") {
		return TOCEntry{
			Type: "whiteout",
			Name: dir + base[len(".wh."):],
		}, nil
	}

	entry := TOCEntry{
		Name:     name,
		Mode:     uint32(hdr.Mode),
		UID:      hdr.Uid,
		GID:      hdr.Gid,
		Uname:    hdr.Uname,
		Gname:    hdr.Gname,
		ModTime:  hdr.ModTime,
		DevMajor: hdr.Devmajor,
		DevMinor: hdr.Devminor,
	}

	// Copy extended attributes from PAX records.
	for k, v := range hdr.PAXRecords {
		if strings.HasPrefix(k, "SCHILY.xattr.") || strings.HasPrefix(k, "user.") {
			if entry.Xattrs == nil {
				entry.Xattrs = make(map[string][]byte)
			}
			entry.Xattrs[k] = []byte(v)
		}
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		entry.Type = "dir"

	case tar.TypeSymlink:
		entry.Type = "symlink"
		entry.LinkName = hdr.Linkname

	case tar.TypeLink:
		entry.Type = "hardlink"
		entry.LinkName = hdr.Linkname

	case tar.TypeChar:
		entry.Type = "char"

	case tar.TypeBlock:
		entry.Type = "block"

	case tar.TypeFifo:
		entry.Type = "fifo"

	default:
		// Regular file (TypeReg, TypeRegA).
		entry.Type = "reg"
		entry.Size = hdr.Size

		if hdr.Size > 0 {
			content, err := io.ReadAll(tr)
			if err != nil {
				return TOCEntry{}, fmt.Errorf("chunked: reading file %q: %w", name, err)
			}
			dgst, err := blobPusher(ctx, content)
			if err != nil {
				return TOCEntry{}, fmt.Errorf("chunked: pushing blob for %q: %w", name, err)
			}
			entry.Digest = dgst
		}
	}

	return entry, nil
}
