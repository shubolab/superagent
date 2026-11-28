package tui

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/term"
)

// Run opens the two sessions and runs the terminal until EOF, Ctrl+D or context cancellation.
func Run(ctx context.Context, in, out *os.File, cfg Config) error {
	a, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	return a.terminal(in, out)
}

func (a *app) terminal(in, out *os.File) (err error) {
	fd := int(in.Fd())
	if !term.IsTerminal(fd) || !term.IsTerminal(int(out.Fd())) {
		return errors.New("需要交互式终端")
	}
	state, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, term.Restore(fd, state)) }()
	if _, err := io.WriteString(out, "\x1b[?1049h\x1b[?7l"); err != nil {
		return err
	}
	defer func() {
		_, _ = io.WriteString(out, "\x1b[?7h\x1b[?1049l")
		for i, unsent := range a.unsent {
			if unsent {
				_, _ = fmt.Fprintf(out, "%s · 未发送\r\n", plain(string(a.cfg.Sessions[i])))
			}
		}
	}()
	ctx, cancel := context.WithCancel(a.ctx)
	defer cancel()
	a.ctx = ctx
	keys := readKeys(ctx, in)
	a.loadSessions()
	for {
		columns, rows, _ := term.GetSize(int(out.Fd()))
		if _, err := io.WriteString(out, a.frame(rows, columns)); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case key, ok := <-keys:
			if !ok || key == '\x04' || key == '\x03' {
				a.shutdown()
				return nil
			}
			a.key(key)
		case update := <-a.inbox:
			a.apply(update)
		}
	}
}

func readKeys(ctx context.Context, in io.Reader) <-chan rune {
	input, keys := make(chan rune), make(chan rune)
	go func() {
		defer close(input)
		reader := bufio.NewReader(in)
		for {
			r, _, err := reader.ReadRune()
			if err != nil || !sendKey(ctx, input, r) || r == '\x04' || r == '\x03' {
				return
			}
		}
	}()
	go func() {
		defer close(keys)
		parseKeys(ctx, input, keys)
	}()
	return keys
}

func sendKey(ctx context.Context, out chan<- rune, key rune) bool {
	select {
	case out <- key:
		return true
	case <-ctx.Done():
		return false
	}
}

// Escape is also the prefix of arrow/function keys. Only a standalone Escape cancels.
func parseKeys(ctx context.Context, input <-chan rune, keys chan<- rune) {
	const escapeDelay = 40 * time.Millisecond
	const (
		normal = iota
		escape
		sequence
	)
	mode := normal
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var timeout <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			if mode == escape && !sendKey(ctx, keys, '\x1b') {
				return
			}
			mode, timeout = normal, nil
		case key, ok := <-input:
			if !ok {
				if mode == escape {
					sendKey(ctx, keys, '\x1b')
				}
				return
			}
			if mode == sequence {
				if key >= 0x40 && key <= 0x7e {
					timer.Stop()
					mode, timeout = normal, nil
				}
				continue
			}
			if mode == escape {
				timer.Stop()
				mode, timeout = normal, nil
				if key == '[' || key == 'O' {
					mode = sequence
					timer.Reset(escapeDelay)
					timeout = timer.C
					continue
				}
				if !sendKey(ctx, keys, '\x1b') {
					return
				}
			}
			if key == '\x1b' {
				mode = escape
				timer.Reset(escapeDelay)
				timeout = timer.C
			} else if !sendKey(ctx, keys, key) {
				return
			}
		}
	}
}
