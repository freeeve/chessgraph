package logx

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// NewLogger returns a zerolog logger configured for console output.
func NewLogger() zerolog.Logger {
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		// Extract just the filename, not the full path
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		// Pad to 28 characters for alignment
		return fmt.Sprintf("%-28s", fmt.Sprintf("%s:%d", short, line))
	}
	logger := zerolog.New(output).With().Timestamp().Caller().Logger()
	return logger
}
