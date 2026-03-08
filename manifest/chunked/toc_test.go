package chunked

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTOC(t *testing.T) {
	t.Run("regular file single chunk", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"reg","name":"foo.txt","size":100,"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		assert.Equal(t, 1, toc.Version)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "reg", toc.Entries[0].Type)
		assert.Equal(t, "foo.txt", toc.Entries[0].Name)
		assert.Equal(t, int64(100), toc.Entries[0].Size)
	})

	t.Run("dir entry", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"dir","name":"mydir/","mode":493}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "dir", toc.Entries[0].Type)
		assert.Equal(t, "mydir/", toc.Entries[0].Name)
		assert.Equal(t, uint32(493), toc.Entries[0].Mode)
	})

	t.Run("symlink entry", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"symlink","name":"link","linkName":"target"}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "symlink", toc.Entries[0].Type)
		assert.Equal(t, "target", toc.Entries[0].LinkName)
	})

	t.Run("hardlink entry", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"hardlink","name":"hard","linkName":"src"}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "hardlink", toc.Entries[0].Type)
		assert.Equal(t, "src", toc.Entries[0].LinkName)
	})

	t.Run("whiteout entry", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"whiteout","name":"deleted.txt"}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "whiteout", toc.Entries[0].Type)
		assert.Equal(t, "deleted.txt", toc.Entries[0].Name)
	})

	t.Run("opaque whiteout entry", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"opaque","name":"dir/"}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "opaque", toc.Entries[0].Type)
		assert.Equal(t, "dir/", toc.Entries[0].Name)
	})

	t.Run("multi-chunk file", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[{"type":"reg","name":"big.bin","size":200,"chunks":[{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","offset":0,"size":100},{"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","offset":100,"size":100}]}]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		require.Len(t, toc.Entries, 1)
		assert.Equal(t, "reg", toc.Entries[0].Type)
		require.Len(t, toc.Entries[0].Chunks, 2)
		assert.Equal(t, int64(100), toc.Entries[0].Chunks[0].Size)
		assert.Equal(t, int64(100), toc.Entries[0].Chunks[1].Offset)
	})

	t.Run("empty entries list", func(t *testing.T) {
		data := []byte(`{"version":1,"entries":[]}`)
		toc, err := ParseTOC(data)
		require.NoError(t, err)
		assert.Empty(t, toc.Entries)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := ParseTOC([]byte(`not json`))
		assert.Error(t, err)
	})

	t.Run("truncated JSON", func(t *testing.T) {
		_, err := ParseTOC([]byte(`{"version":1,"entries":[`))
		assert.Error(t, err)
	})
}

func TestChunkDigests(t *testing.T) {
	dgst1 := digest.Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dgst2 := digest.Digest("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	dgst3 := digest.Digest("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	t.Run("single chunk reg file", func(t *testing.T) {
		toc := &TOC{
			Version: 1,
			Entries: []TOCEntry{
				{Type: "reg", Name: "file.txt", Size: 100, Digest: dgst1},
			},
		}
		digests := toc.ChunkDigests()
		assert.Equal(t, []digest.Digest{dgst1}, digests)
	})

	t.Run("multi-chunk reg file", func(t *testing.T) {
		toc := &TOC{
			Version: 1,
			Entries: []TOCEntry{
				{
					Type: "reg", Name: "large.bin", Size: 200,
					Chunks: []TOCChunk{
						{Digest: dgst1, Offset: 0, Size: 100},
						{Digest: dgst2, Offset: 100, Size: 100},
					},
				},
			},
		}
		digests := toc.ChunkDigests()
		assert.Equal(t, []digest.Digest{dgst1, dgst2}, digests)
	})

	t.Run("empty reg file excluded", func(t *testing.T) {
		toc := &TOC{
			Version: 1,
			Entries: []TOCEntry{
				{Type: "reg", Name: "empty.txt", Size: 0},
			},
		}
		digests := toc.ChunkDigests()
		assert.Empty(t, digests)
	})

	t.Run("non-reg entries excluded", func(t *testing.T) {
		toc := &TOC{
			Version: 1,
			Entries: []TOCEntry{
				{Type: "dir", Name: "mydir/"},
				{Type: "symlink", Name: "link", LinkName: "target"},
				{Type: "hardlink", Name: "hard", LinkName: "src"},
				{Type: "whiteout", Name: "deleted"},
				{Type: "opaque", Name: "dir/"},
				{Type: "reg", Name: "file.txt", Size: 50, Digest: dgst3},
			},
		}
		digests := toc.ChunkDigests()
		assert.Equal(t, []digest.Digest{dgst3}, digests)
	})

	t.Run("empty TOC", func(t *testing.T) {
		toc := &TOC{Version: 1, Entries: nil}
		digests := toc.ChunkDigests()
		assert.Empty(t, digests)
	})

	t.Run("multiple files", func(t *testing.T) {
		toc := &TOC{
			Version: 1,
			Entries: []TOCEntry{
				{Type: "reg", Name: "a.txt", Size: 10, Digest: dgst1},
				{Type: "reg", Name: "b.txt", Size: 0},
				{Type: "reg", Name: "c.txt", Size: 20, Digest: dgst2},
			},
		}
		digests := toc.ChunkDigests()
		assert.Equal(t, []digest.Digest{dgst1, dgst2}, digests)
	})
}
