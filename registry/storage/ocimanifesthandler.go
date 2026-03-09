package storage

import (
	"context"
	"fmt"
	"net/url"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/internal/dcontext"
	chunked "github.com/clipper-registry/clipper-oci"
	"github.com/distribution/distribution/v3/manifest/ocischema"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// ocischemaManifestHandler is a ManifestHandler that covers ocischema manifests.
type ocischemaManifestHandler struct {
	repository   distribution.Repository
	blobStore    distribution.BlobStore
	ctx          context.Context
	manifestURLs manifestURLs

	// pushPolicy controls behaviour for non-chunked pushes.
	// Values: "" / "allow" (default), "convert" (stub), "reject".
	pushPolicy string
}

var _ ManifestHandler = &ocischemaManifestHandler{}

func (ms *ocischemaManifestHandler) Unmarshal(ctx context.Context, dgst digest.Digest, content []byte) (distribution.Manifest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*ocischemaManifestHandler).Unmarshal")

	m := &ocischema.DeserializedManifest{}
	if err := m.UnmarshalJSON(content); err != nil {
		return nil, err
	}

	return m, nil
}

func (ms *ocischemaManifestHandler) Put(ctx context.Context, manifest distribution.Manifest, skipDependencyVerification bool) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*ocischemaManifestHandler).Put")

	m, ok := manifest.(*ocischema.DeserializedManifest)
	if !ok {
		return "", fmt.Errorf("non-ocischema manifest put to ocischemaManifestHandler: %T", manifest)
	}

	// If convert policy is set and there are traditional layers, attempt conversion.
	if ms.pushPolicy == "convert" && hasTraditionalLayers(m) {
		newDgst, err := ms.convertAndPut(ctx, m)
		if err != nil {
			// Stub: log and fall through to normal put.
			dcontext.GetLogger(ctx).Warnf("chunked convert stub: conversion not fully implemented, falling through: %v", err)
		} else {
			// Populate chunk index from converted manifest.
			ms.populateChunkIndex(ctx, m)
			return newDgst, nil
		}
	}

	if err := ms.verifyManifest(ms.ctx, *m, skipDependencyVerification); err != nil {
		return "", err
	}

	mt, payload, err := m.Payload()
	if err != nil {
		return "", err
	}

	revision, err := ms.blobStore.Put(ctx, mt, payload)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error putting payload into blobstore: %v", err)
		return "", err
	}

	// Populate chunk index for TOC layers in this manifest.
	ms.populateChunkIndex(ctx, m)

	return revision.Digest, nil
}

// hasTraditionalLayers returns true if the manifest has any non-TOC layer media types.
func hasTraditionalLayers(m *ocischema.DeserializedManifest) bool {
	for _, layer := range m.Manifest.Layers {
		mt := layer.MediaType
		if mt != chunked.MediaTypeLayerTOC {
			switch mt {
			case v1.MediaTypeImageLayer, v1.MediaTypeImageLayerGzip,
				v1.MediaTypeImageLayerNonDistributable, v1.MediaTypeImageLayerNonDistributableGzip: //nolint:staticcheck
				return true
			}
		}
	}
	return false
}

// populateChunkIndex adds all chunk digests from TOC layers to the global chunk index.
func (ms *ocischemaManifestHandler) populateChunkIndex(ctx context.Context, m *ocischema.DeserializedManifest) {
	repoName := ms.repository.Named().Name()
	blobsService := ms.repository.Blobs(ctx)

	for _, descriptor := range m.Manifest.Layers {
		if descriptor.MediaType != chunked.MediaTypeLayerTOC {
			continue
		}
		data, err := blobsService.Get(ctx, descriptor.Digest)
		if err != nil {
			dcontext.GetLogger(ctx).Warnf("chunked: failed to get TOC blob %s for chunk index: %v", descriptor.Digest, err)
			continue
		}
		toc, err := chunked.ParseTOC(data)
		if err != nil {
			dcontext.GetLogger(ctx).Warnf("chunked: failed to parse TOC blob %s: %v", descriptor.Digest, err)
			continue
		}
		DefaultChunkIndex.Add(repoName, toc.ChunkDigests())
	}
}

// convertAndPut converts all traditional layers to chunked format and stores the
// converted manifest. This is a stub in Phase 1.
func (ms *ocischemaManifestHandler) convertAndPut(ctx context.Context, m *ocischema.DeserializedManifest) (digest.Digest, error) {
	// Phase 1 stub: full in-memory conversion is deferred.
	// TODO: implement full conversion when Phase 2 is ready.
	return "", fmt.Errorf("chunked: convert policy not fully implemented in Phase 1")
}

// verifyManifest ensures that the manifest content is valid from the
// perspective of the registry. As a policy, the registry only tries to store
// valid content, leaving trust policies of that content up to consumers.
func (ms *ocischemaManifestHandler) verifyManifest(ctx context.Context, mnfst ocischema.DeserializedManifest, skipDependencyVerification bool) error {
	var errs distribution.ErrManifestVerification

	if mnfst.Manifest.SchemaVersion != 2 {
		return fmt.Errorf("unrecognized manifest schema version %d", mnfst.Manifest.SchemaVersion)
	}

	if skipDependencyVerification {
		return nil
	}

	manifestService, err := ms.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	blobsService := ms.repository.Blobs(ctx)

	for _, descriptor := range mnfst.References() {
		err := descriptor.Digest.Validate()
		if err != nil {
			errs = append(errs, err, distribution.ErrManifestBlobUnknown{Digest: descriptor.Digest})
			continue
		}

		switch descriptor.MediaType {
		case chunked.MediaTypeLayerTOC:
			// Fetch and parse the TOC blob, then verify all chunk digests exist.
			data, fetchErr := blobsService.Get(ctx, descriptor.Digest)
			if fetchErr != nil {
				errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: descriptor.Digest})
				continue
			}
			toc, parseErr := chunked.ParseTOC(data)
			if parseErr != nil {
				errs = append(errs, parseErr, distribution.ErrManifestBlobUnknown{Digest: descriptor.Digest})
				continue
			}
			for _, chunkDgst := range toc.ChunkDigests() {
				if _, statErr := blobsService.Stat(ctx, chunkDgst); statErr != nil {
					errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: chunkDgst})
				}
			}

		case v1.MediaTypeImageLayer, v1.MediaTypeImageLayerGzip, v1.MediaTypeImageLayerNonDistributable, v1.MediaTypeImageLayerNonDistributableGzip: //nolint:staticcheck // ignore A1019: v1.MediaTypeImageLayerNonDistributable is deprecated: Non-distributable layers are deprecated, and not recommended for future use.
			allow := ms.manifestURLs.allow
			deny := ms.manifestURLs.deny
			for _, u := range descriptor.URLs {
				var pu *url.URL
				pu, err = url.Parse(u)
				if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Fragment != "" || (allow != nil && !allow.MatchString(u)) || (deny != nil && deny.MatchString(u)) {
					err = errInvalidURL
					break
				}
			}
			if err == nil {
				// check the presence if it is normal layer or
				// there is no urls for non-distributable
				if len(descriptor.URLs) == 0 ||
					(descriptor.MediaType == v1.MediaTypeImageLayer || descriptor.MediaType == v1.MediaTypeImageLayerGzip) {

					_, err = blobsService.Stat(ctx, descriptor.Digest)
				}
			}

		case v1.MediaTypeImageManifest:
			var exists bool
			exists, err = manifestService.Exists(ctx, descriptor.Digest)
			if err != nil || !exists {
				err = distribution.ErrBlobUnknown // just coerce to unknown.
			}

			if err != nil {
				dcontext.GetLogger(ms.ctx).WithError(err).Debugf("failed to ensure exists of %v in manifest service", descriptor.Digest)
			}
			fallthrough // double check the blob store.
		default:
			// check the presence
			_, err = blobsService.Stat(ctx, descriptor.Digest)
		}

		if err != nil {
			if err != distribution.ErrBlobUnknown {
				errs = append(errs, err)
			}

			// On error here, we always append unknown blob errors.
			errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: descriptor.Digest})
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

