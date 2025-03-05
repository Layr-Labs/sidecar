// Package snapshot provides functionality for creating and restoring database snapshots.
package snapshot

// Result represents the outcome of a snapshot operation, containing information about
// the operation's output, any errors encountered, and the command executed.
type Result struct {
	// Mime is the MIME type of the output file
	Mime        string
	// File is the path to the output file
	File        string
	// Output contains the standard output from the command
	Output      string
	// Error contains error information if the operation failed
	Error       *ResultError
	// FullCommand is the complete command that was executed
	FullCommand string
}

// ResultError contains detailed information about an error that occurred during
// a snapshot operation.
type ResultError struct {
	// Err is the underlying error
	Err       error
	// CmdOutput contains the command's error output
	CmdOutput string
	// ExitCode is the exit code returned by the command
	ExitCode  int
}
