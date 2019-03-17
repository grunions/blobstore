package blobstore

import (
	"fmt"
	"os"
	"strconv"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// S3Config contains the configuration for an S3 backend
type S3Config struct {
	Key      string
	Secret   string
	Location string
	Bucket   string
	Endpoint string
	SSL      bool
}

//func NewBlobStore(config S3Config) *BlobStore {
//
//}

// Future methods?
// Put(blob) error {}
// Stat(blob) (ObjectInfo, error) {}
// Get(blob) (io.ReadCloser, error) {}
// Delete(blob) error {}

// S3 is a Blobstore wrapping an S3 compatible storage
type S3 struct {
	config *S3Config
	client *minio.Client
}

// NewS3 returns a new S3 bobstore
func NewS3(config *S3Config) *S3 {
	return &S3{
		config: config,
	}
}

func (s3 *S3) getClient() *minio.Client {
	if s3.client != nil {
		return s3.client
	}

	client, err := minio.New(
		s3.config.Endpoint,
		s3.config.Key,
		s3.config.Secret,
		s3.config.SSL)
	if err != nil {
		panic(err)
	}

	s3.client = client
	return client
}

// CheckDuplicate return true if a duplicate exists
func (s3 *S3) CheckDuplicate(blob *LocalBlob) bool {
	remoteFilename := fmt.Sprintf("blob/%x.gz", blob.Hash())

	client := s3.getClient()
	o, err := client.GetObject(s3.config.Bucket, remoteFilename, minio.GetObjectOptions{})
	if err != nil {
		return false

	}

	var info minio.ObjectInfo
	if info, err = o.Stat(); err != nil {
		return false
	}

	if blob.Size() != info.Size {
		// size does not match
		return false
	}

	// found
	return true
}

// UploadBlob will take a blob, and upload it to the store (referenced by its hash)
func (s3 *S3) UploadBlob(blob *LocalBlob) error {
	remoteFilename := fmt.Sprintf("blob/%x.gz", blob.Hash())

	bar := pb.New64(blob.Size())
	bar.ShowSpeed = true
	bar.ShowElapsedTime = true
	bar.ShowTimeLeft = true
	bar.Units = pb.U_BYTES
	bar.ShowFinalTime = true
	bar.Start()
	defer bar.Finish()

	client := s3.getClient()

	written, err := client.FPutObject(
		s3.config.Bucket,
		remoteFilename,
		blob.File.Name(),
		minio.PutObjectOptions{
			Progress:    bar,
			ContentType: "application/gzip",
			UserMetadata: map[string]string{
				"Uncompressed-Size": strconv.FormatInt(blob.UncompressedSize(), 10),
				"Reference-Name":    blob.Reference,
				"Is-Dir":            strconv.FormatBool(blob.IsDir),
			},
		})
	bar.Set64(written)
	if err != nil {
		// try to remove
		client.RemoveObject(s3.config.Bucket, remoteFilename)
		return errors.Wrap(err, "Error while uploading blob")
	}

	return nil
}

// UploadDir will take an entire local dir, and upload it to the store,
// returning its reference/checksum
func (s3 *S3) UploadDir(src string) ([]byte, error) {
	blob, err := NewLocalBlob()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to prepare dir blob")
	}
	blob.IsDir = true
	defer os.Remove(blob.File.Name())
	defer blob.Close()

	if err := TarDir(src, blob); err != nil {
		return nil, errors.Wrap(err, "Failed to tar dir")
	}
	if err := blob.Close(); err != nil {
		return nil, errors.Wrap(err, "Failed to flush blob dir")
	}

	if s3.CheckDuplicate(blob) {
		// already exists, exit early
		return blob.Hash(), nil
	}

	if err := s3.UploadBlob(blob); err != nil {
		return blob.Hash(), errors.Wrap(err, "Failed to upload dir")
	}

	return blob.Hash(), nil
}
