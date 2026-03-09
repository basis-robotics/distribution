package clipperoci

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/opencontainers/go-digest"
)

const (
	// MediaTypeLayerTOC is the media type for chunked layer TOC blobs.
	MediaTypeLayerTOC = "application/vnd.oci.image.layer.toc.v1+json"

	// OriginalDigestAnnotation is the annotation key for the original layer digest.
	OriginalDigestAnnotation = "org.opencontainers.image.layer.original-digest"

	// UncompressedSizeAnnotation is the annotation key for the total uncompressed
	// size of all regular files in the layer, in bytes.
	UncompressedSizeAnnotation = "org.opencontainers.image.layer.uncompressed-size"
)

// TOC is the top-level table of contents for a chunked layer.
type TOC struct {
	Version int        `json:"version"`
	Entries []TOCEntry `json:"entries"`
}

// TOCEntry represents a single entry in the chunked layer TOC.
type TOCEntry struct {
	// Type is the entry type: "reg", "dir", "symlink", "hardlink", "char", "block", "fifo", "whiteout", "opaque".
	Type string `json:"type"`

	// Name is the path of the entry within the layer.
	Name string `json:"name"`

	// Size is the uncompressed size in bytes (for "reg" entries).
	Size int64 `json:"size,omitempty"`

	// Mode is the file mode and permission bits.
	Mode uint32 `json:"mode,omitempty"`

	// UID is the numeric user ID of the owner.
	UID int `json:"uid,omitempty"`

	// GID is the numeric group ID of the owner.
	GID int `json:"gid,omitempty"`

	// Uname is the user name of the owner.
	Uname string `json:"uname,omitempty"`

	// Gname is the group name of the owner.
	Gname string `json:"gname,omitempty"`

	// ModTime is the modification time of the entry.
	ModTime time.Time `json:"modtime,omitempty"`

	// LinkName is the target of a symlink or the source name of a hardlink.
	LinkName string `json:"linkName,omitempty"`

	// DevMajor is the major device number for char/block device entries.
	DevMajor int64 `json:"devMajor,omitempty"`

	// DevMinor is the minor device number for char/block device entries.
	DevMinor int64 `json:"devMinor,omitempty"`

	// Xattrs holds extended attributes for the entry.
	Xattrs map[string][]byte `json:"xattrs,omitempty"`

	// Digest is the content-addressed digest of a regular file stored as a single chunk blob.
	Digest digest.Digest `json:"digest,omitempty"`

	// Chunks holds chunk descriptors when a file is split across multiple chunk blobs.
	Chunks []TOCChunk `json:"chunks,omitempty"`
}

// TOCChunk describes one chunk blob of a multi-chunk regular file.
// It serializes as a [digest, size] JSON array.
type TOCChunk struct {
	// Digest is the content-addressed digest of this chunk blob.
	Digest digest.Digest

	// Size is the byte length of this chunk.
	Size int64
}

func (c TOCChunk) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]interface{}{c.Digest, c.Size})
}

func (c *TOCChunk) UnmarshalJSON(data []byte) error {
	var arr [2]json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("TOCChunk: expected [digest, size] array: %w", err)
	}
	digestRaw, sizeRaw := arr[0], arr[1]
	if err := json.Unmarshal(digestRaw, &c.Digest); err != nil {
		return fmt.Errorf("TOCChunk: invalid digest: %w", err)
	}
	return json.Unmarshal(sizeRaw, &c.Size)
}

// ParseTOC parses a TOC from JSON-encoded bytes.
func ParseTOC(data []byte) (*TOC, error) {
	var toc TOC
	if err := json.Unmarshal(data, &toc); err != nil {
		return nil, fmt.Errorf("chunked: invalid TOC JSON: %w", err)
	}
	return &toc, nil
}

// UncompressedSize returns the total uncompressed size of all regular files in the TOC.
func (t *TOC) UncompressedSize() int64 {
	var total int64
	for _, entry := range t.Entries {
		if entry.Type == "reg" {
			total += entry.Size
		}
	}
	return total
}

// ChunkDigests returns all chunk blob digests referenced by this TOC.
// It includes single-file digests and per-chunk digests for "reg" entries with size > 0.
func (t *TOC) ChunkDigests() []digest.Digest {
	var digests []digest.Digest
	for _, entry := range t.Entries {
		if entry.Type != "reg" || entry.Size == 0 {
			continue
		}
		if entry.Digest != "" {
			digests = append(digests, entry.Digest)
		}
		for _, chunk := range entry.Chunks {
			if chunk.Digest != "" {
				digests = append(digests, chunk.Digest)
			}
		}
	}
	return digests
}
