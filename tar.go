package blobstore

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/pgzip"

	"github.com/pkg/errors"
)

func TarDir(src string, writer io.Writer) error {
	// ensure the src actually exists before trying to tar it
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("Unable to tar files - %v", err.Error())
	}

	tw := tar.NewWriter(writer)
	defer tw.Close()

	// reusable buffer for io.CopyBuffer
	copyBuffer := make([]byte, 32*1024)

	// walk path
	return filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {

		// return on any error
		if err != nil {
			return err
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// reset modification time, to make output deterministic
		header.ModTime = time.Time{}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(strings.Replace(file, src, "", -1), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// return on non-regular files)
		if !fi.Mode().IsRegular() {
			return nil
		}

		// open files for taring
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := io.CopyBuffer(tw, f, copyBuffer); err != nil {
			return err
		}

		// manually close here after each file operation; defering would cause each file close
		// to wait until all operations have completed.
		f.Close()

		return nil
	})
}

// Untargz takes a destination path and a reader; a tar reader loops over the tarfile
// creating the file structure at 'dst' along the way, and writing any files
func Untargz(dst string, r io.Reader) error {

	gzr, err := pgzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// reusable buffer for io.CopyBuffer
	copyBuffer := make([]byte, 32*1024)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}

		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// copy over contents
			if _, err := io.CopyBuffer(f, tr, copyBuffer); err != nil {
				return err
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()

		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}

		default:
			log.Print("Tar: ignoring unknown tar header")
		}
	}
}

func TarZip(reader io.ReaderAt, size int64, writer io.Writer) error {
	zr, err := zip.NewReader(reader, size)
	if err != nil {
		return errors.Wrap(err, "Could not open zip reader")
	}

	tw := tar.NewWriter(writer)
	defer tw.Close()

	copyBuffer := make([]byte, 32*1024)

	for _, f := range zr.File {
		fi := f.FileInfo()

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// reset modification time, to make output deterministic
		header.ModTime = time.Time{}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(fi.Name(), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// return on non-regular files)
		if !fi.Mode().IsRegular() {
			return nil
		}

		// open files for taring
		fr, err := f.Open()
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := io.CopyBuffer(tw, fr, copyBuffer); err != nil {
			return err
		}

		// manually close here after each file operation; defering would cause each file close
		// to wait until all operations have completed.
		fr.Close()
	}

	return tw.Close()
}
