package tui

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	sdk "github.com/shubolab/superagent"
)

const maxPreviewBytes = 16 << 10
const maxViewLines = 100

type preview struct {
	step uint32
	text string
}

func (op *operation) observe(event sdk.Event) {
	switch event.Kind {
	case sdk.EventTextDelta:
		op.updatePreview(event.Step, event.TextDelta, false)
	case sdk.EventMessageCompleted:
		if event.Message != nil {
			op.updatePreview(event.Step, event.Message.Text, true)
		}
	case sdk.EventToolDispatched:
		if event.Tool != nil {
			op.tool = string(event.Tool.Tool)
		}
	case sdk.EventToolCompleted:
		op.tool = ""
	}
}

func (op *operation) updatePreview(step uint32, text string, replace bool) {
	index := slices.IndexFunc(op.preview, func(p preview) bool { return p.step == step })
	if index == -1 {
		if len(op.preview) >= maxViewLines {
			op.truncated = true
			return
		}
		index = len(op.preview)
		op.preview = append(op.preview, preview{step: step})
	}
	entry := &op.preview[index]
	if replace {
		op.previewBytes -= len(entry.text)
		entry.text = ""
	}
	part := prefix(text, maxPreviewBytes-op.previewBytes)
	op.truncated = op.truncated || len(part) != len(text)
	entry.text += part
	op.previewBytes += len(part)
}

func (a *app) frame(rows int) string {
	if rows < 6 {
		rows = 24
	}
	var out strings.Builder
	out.WriteString("\x1b[H\x1b[2J")
	for i, id := range a.cfg.Sessions {
		if i > 0 {
			out.WriteString("   ")
		}
		label := fmt.Sprintf("%s · %s", plain(string(id)), a.views[i].status)
		if i == a.selected {
			fmt.Fprintf(&out, "[%s]", label)
		} else {
			out.WriteString(label)
		}
	}
	out.WriteString("\r\n\r\n")
	v := &a.views[a.selected]
	lines := slices.Clone(v.history)
	if op := v.op; op != nil {
		if op.terminal != nil {
			lines = append(lines, op.finalLines...)
		} else {
			if op.input != "" {
				lines = append(lines, labeledLines("你", op.input)...)
			}
			for _, p := range op.preview {
				if p.text != "" {
					lines = append(lines, labeledLines("助手", p.text)...)
				}
			}
			if op.tool != "" {
				lines = append(lines, "工具 · "+plain(op.tool))
			}
			if op.truncated {
				lines = append(lines, "预览已截断")
			}
			if op.interrupted {
				lines = append(lines, "观察中断，等待结果")
			}
		}
	}
	visible := max(1, rows-4)
	for _, line := range lines[max(0, len(lines)-visible):] {
		out.WriteString(line)
		out.WriteString("\r\n")
	}
	out.WriteString("> ")
	out.WriteString(plain(string(v.draft)))
	out.WriteString("\x1b7\r\n")
	if v.status == "待保存" {
		out.WriteString("Tab 切换   Enter 重试保存   Ctrl+D 退出")
	} else {
		out.WriteString("Tab 切换   Enter 发送   Esc 取消当前   Ctrl+D 退出")
	}
	out.WriteString("\x1b8")
	return out.String()
}

func messageLines(messages []sdk.Message) []string {
	var result []string
	for _, message := range messages {
		switch {
		case message.User != nil:
			result = historyTail(result, labeledLines("你", message.User.Text))
		case message.Assistant != nil && message.Assistant.Text != "":
			result = historyTail(result, labeledLines("助手", message.Assistant.Text))
		case message.Tool != nil:
			result = historyTail(result, []string{"工具 · " + plain(string(message.Tool.Tool))})
		}
	}
	return result
}

func labeledLines(label, text string) []string {
	text = prefix(text, maxPreviewBytes)
	split := strings.Split(label+" · "+text, "\n")
	start := max(0, len(split)-maxViewLines)
	lines := make([]string, 0, len(split)-start)
	for _, line := range split[start:] {
		lines = append(lines, plain(line))
	}
	return lines
}

func historyTail(history, delta []string) []string {
	keep := max(0, maxViewLines-len(delta))
	return slices.Concat(history[max(0, len(history)-keep):], delta[max(0, len(delta)-maxViewLines):])
}

func plain(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
}

func prefix(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	for limit > 0 && !utf8.RuneStart(text[limit]) {
		limit--
	}
	return text[:limit]
}
