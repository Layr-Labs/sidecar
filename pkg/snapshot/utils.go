package snapshot

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"io"
	"os"
	"os/exec"
	"strings"
)

func openFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0775)
	if err != nil {
		// Handle error
		return nil, err
	}
	return file, nil
}

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

func getCmdPath(cmd string) (string, error) {
	return exec.LookPath(cmd)
}

func cmdExists(cmd string) bool {
	_, err := getCmdPath(cmd)

	return err == nil
}
