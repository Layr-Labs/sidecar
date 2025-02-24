package snapshot

type Result struct {
	Mime        string
	File        string
	Output      string
	Error       *ResultError
	FullCommand string
}

type ResultError struct {
	Err       error
	CmdOutput string
	ExitCode  int
}
