package prompt

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// Ask prints msg, reads a line from stdin, trims whitespace, and returns it.
//
// It panics only if reading from stdin fails — suitable for CLIs
// where I/O errors are exceptional.
func Ask(msg string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(msg)

	input, err := reader.ReadString('\n')
	if err != nil {
		panic(fmt.Errorf("reading stdin: %w", err))
	}
	return strings.TrimSpace(input)
}

// AskDefault behaves like Ask, but returns def if the user provides empty input.
func AskDefault(msg, def string) string {
	answer := Ask(msg)
	if answer == "" {
		return def
	}
	return answer
}

// AskPassword prompts without echoing what the user types.
//
// Falls back to plain Ask when stdin / stdout isn’t a TTY (e.g. piped input).
func AskPassword(msg string) string {
	// If stdin isn't a terminal (e.g. piped), fall back to visible input.
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return Ask(msg) // visibility is unavoidable here
	}

	fmt.Print(msg)
	bytePwd, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println() // newline after user hits ENTER
	if err != nil {
		panic(fmt.Errorf("password prompt: %w", err))
	}
	return strings.TrimSpace(string(bytePwd))
}

// Confirm asks a yes/no question, returning true for yes.
// Accepts y/yes/Y/YES and n/no/etc (case-insensitive).
func Confirm(msg string) bool {
	for {
		answer := strings.ToLower(Ask(msg + " [y/n]: "))
		switch answer {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			fmt.Println("Please type 'y' or 'n' and press ENTER.")
		}
	}
}

// ErrCancelled is returned when the user explicitly aborts at a confirmation.
// Up to you whether you use it; shows one pattern for graceful exits.
var ErrCancelled = errors.New("operation cancelled by user")
