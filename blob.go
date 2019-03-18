package blobstore

import (
	"crypto/sha256"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/klauspost/pgzip"

	"github.com/miolini/datacounter"
	"github.com/pkg/errors"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// LocalBlob is a gzip compressed object, which may either be a single file
// or a directory in a tar file
type LocalBlob struct {
	IsDir     bool
	Reference string
	// Size()
	// UncompressedSize()
	// Hash()

	File *os.File

	pw *pb.ProgressBar

	gw  io.WriteCloser             // gzip writer for compression
	hw  hash.Hash                  // hashwriter for checksum
	ccw *datacounter.WriterCounter // countWriter for counting written compressed bytes
	ucw *datacounter.WriterCounter // countWriter for counting written uncompressed bytes
	mw  io.Writer                  // multiWriter for combining hash and gzip

	// A human readable reference, for example a filename associated with the
	// blob, e.g. "Human Music.mp3". This is non-unique, user-controlled and
	// must not be used for any logic.
}

// NewLocalBlob creates a new blob with a temporary file, which MUST be
// deleted after all related actions are complete.
func NewLocalBlob() (*LocalBlob, error) {
	blob := &LocalBlob{
		IsDir: false,
	}

	var err error

	blob.File, err = ioutil.TempFile("", "blob")
	if err != nil {
		return nil, errors.Wrap(err, "Blob: could not create temporary file")
	}

	// progress bar
	blob.pw = pb.New(0)
	blob.pw.SetUnits(pb.U_BYTES)
	blob.pw.ShowSpeed = true
	blob.pw.ShowPercent = false
	blob.pw.ShowTimeLeft = false
	blob.pw.ShowBar = false
	blob.pw.Start()

	blob.ccw = datacounter.NewWriterCounter(blob.File)
	blob.gw, _ = pgzip.NewWriterLevel(blob.ccw, pgzip.BestCompression)
	blob.ucw = datacounter.NewWriterCounter(blob.gw)
	blob.hw = sha256.New()
	blob.mw = io.MultiWriter(blob.ucw, blob.hw, blob.pw)

	return blob, nil
}

// Close finishes the writing process to the blob
func (blob *LocalBlob) Close() error {
	blob.pw.Finish()
	blob.gw.Close()
	return blob.File.Close()
}

// Size returns the Compressed blob size
func (blob *LocalBlob) Size() int64 {
	return int64(blob.ccw.Count())
}

// UncompressedSize returns the original size, or the size of the
// Tar file if the blob is a dir blob
func (blob *LocalBlob) UncompressedSize() int64 {
	return int64(blob.ucw.Count())
}

// Hash returns the checksum of the uncompressed data
func (blob *LocalBlob) Hash() []byte {
	return blob.hw.Sum(nil)
}

// Write implements the standard Write interface
func (blob *LocalBlob) Write(b []byte) (n int, err error) {
	return blob.mw.Write(b)
}

func ReaderToBlob(fr io.Reader) (blob *LocalBlob, e error) {

	blob, err := NewLocalBlob()
	if err != nil {
		blob.Close()
		os.Remove(blob.File.Name()) // try to clean up
		return nil, errors.Wrap(err, "Could not create blob")
	}
	defer blob.Close()

	// copy file reader into the chain
	_, err = io.Copy(blob, fr)
	if err != nil {
		os.Remove(blob.File.Name()) // try to clean up
		return nil, errors.Wrap(err, "Error while processing")
	}

	blob.Close() // flush all remaining bytes

	return blob, nil
}
