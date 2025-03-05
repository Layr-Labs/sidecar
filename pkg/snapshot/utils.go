package snapshot

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"io"
	"os"
	"os/exec"
	"strings"
)

// openFile creates or truncates a file at the specified path with write permissions.
// It returns a file handle and any error encountered during the operation.
func openFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0775)
	if err != nil {
		// Handle error
		return nil, err
	}
	return file, nil
}

// streamErrorOutput reads from the provided ReadCloser and stores the output in the Result.
// It uses a 32KB buffer to efficiently read the error output stream.
// If an error occurs during reading, it populates the Result's Error field.
func streamErrorOutput(out io.ReadCloser, result *Result) {
	var output strings.Builder
	buffer := make([]byte, 32*1024) // 32KB buffer
	for {
		n, err := out.Read(buffer)
		if n > 0 {
			output.Write(buffer[:n])
		}
		if err == io.EOF {
			result.Output = output.String()
			return
		}
		if err != nil {
			result.Error = &ResultError{
				Err:       fmt.Errorf("error reading output: %w", err),
				CmdOutput: output.String(),
			}
			return
		}
	}
}

// streamStdout reads from the provided ReadCloser and writes the content to either
// a file specified by outputFileName or to stdout if outputFileName is empty.
// It displays a progress bar during the operation and uses a 4MB buffer for efficient I/O.
func streamStdout(out io.ReadCloser, outputFileName string) {
	var dest io.Writer
	if outputFileName != "" {
		file, err := openFile(outputFileName)
		if err != nil {
			fmt.Printf("error opening output file: %v\n", err)
			return
		}
		dest = file
		defer file.Close()
	} else {
		dest = os.Stdout
	}

	bar := progressbar.DefaultBytes(-1, fmt.Sprintf("writing %s", outputFileName))
	defer func() {
		// print a newline after the progress bar is done to make the output look nice
		fmt.Println()
	}()

	buffer := make([]byte, 4*1024*1024) // 4MB buffer
	for {
		n, err := out.Read(buffer)
		if n > 0 {
			if _, err := io.MultiWriter(dest, bar).Write(buffer[:n]); err != nil {
				fmt.Printf("error writing output: %v\n", err)
				return
			}
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Printf("error reading stdout from exec: %v\n", err)
			return
		}
	}
}

// getCmdPath returns the absolute path to the specified command executable.
// It uses exec.LookPath to search for the command in the system's PATH.
func getCmdPath(cmd string) (string, error) {
	return exec.LookPath(cmd)
}

// cmdExists checks if the specified command exists in the system's PATH.
// It returns true if the command exists and is executable, false otherwise.
func cmdExists(cmd string) bool {
	_, err := getCmdPath(cmd)

	return err == nil
}
