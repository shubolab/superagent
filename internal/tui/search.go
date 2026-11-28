package tui

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"

	sdk "github.com/shubolab/superagent"
)

func (op *operation) updateTool(tool sdk.ToolEvent) {
	if tool.CallID == "" {
		return
	}
	index := slices.IndexFunc(op.tools, func(existing sdk.ToolEvent) bool { return existing.CallID == tool.CallID })
	if index >= 0 {
		op.tools[index] = tool
	} else if len(op.tools) < maxViewLines {
		op.tools = append(op.tools, tool)
	} else {
		op.truncated = true
	}
}

func toolEventLine(event sdk.ToolEvent) string {
	label := toolLabel(event.Tool, event.CallID)
	switch event.Status {
	case "":
		if event.Tool == "search" {
			return label + " · 搜索中"
		}
		return label + " · 执行中"
	case sdk.ToolStatusSucceeded:
		return label + " · 已结束 · 待确认"
	case sdk.ToolStatusRejected:
		return label + " · 已拒绝" + toolReason(event.Code)
	case sdk.ToolStatusFailed:
		return label + " · 已失败" + toolReason(event.Code)
	case sdk.ToolStatusCancelled:
		return label + " · 已取消"
	default:
		return label + " · 待核对"
	}
}

func toolLabel(tool sdk.ToolRef, id sdk.CallID) string {
	return "工具 · " + toolText(string(tool), 64) + " [" + toolText(string(id), 64) + "]"
}

func toolResultLines(call sdk.ModelToolCall, message sdk.ToolMessage) []string {
	label := toolLabel(message.Tool, call.CallID)
	result := message.Result
	reason := ""
	if result.Reason != nil {
		reason = toolReason(result.Reason.Code)
	}
	switch {
	case result.State == sdk.ToolExecutionNotExecuted:
		return []string{label + " · 未执行" + reason}
	case result.State != sdk.ToolExecutionKnown || result.Completion == nil:
		return []string{label + " · 待核对"}
	case result.Completion.Outcome == sdk.ToolCancelled:
		return []string{label + " · 已取消" + reason}
	case result.Completion.Outcome == sdk.ToolFailed:
		return []string{label + " · 已失败" + reason}
	case result.Completion.Outcome != sdk.ToolSucceeded:
		return []string{label + " · 待核对"}
	case message.Tool != "search":
		return []string{label + " · 已完成"}
	}
	var report sdk.SearchReport
	content := result.Completion.Content
	if len(content) > maxPreviewBytes || json.Unmarshal([]byte(content), &report) != nil ||
		(report.Coverage != "complete" && report.Coverage != "partial") || report.Matches == nil ||
		result.Usage == nil || result.Usage.ScannedBytes == nil || report.ScannedBytes != *result.Usage.ScannedBytes {
		return []string{label + " · 已完成"}
	}
	var args struct{ Path string }
	if len(call.Arguments) <= maxPreviewBytes && json.Unmarshal(call.Arguments, &args) == nil && args.Path != "" {
		label += " · " + toolText(args.Path, 96)
	}
	var summary string
	if report.Coverage == "complete" {
		summary = fmt.Sprintf("完整 · %d 命中", len(report.Matches))
	} else {
		summary = fmt.Sprintf("部分覆盖%s · 已返回 %d 条", toolReason(report.Reason), len(report.Matches))
	}
	lines := []string{fmt.Sprintf("%s · %s · %d 字节", label, summary, report.ScannedBytes)}
	for _, match := range report.Matches[:min(3, len(report.Matches))] {
		lines = append(lines, fmt.Sprintf("  第 %d 行 · %s", match.Line, toolText(match.Preview, 256)))
	}
	return lines
}

func toolReason(code string) string {
	var label string
	switch code {
	case "scan_limit":
		label = "扫描上限"
	case "line_limit":
		label = "单行上限"
	case "match_limit":
		label = "命中数上限"
	case "preview_limit":
		label = "预览上限"
	case "result_limit":
		label = "回执上限"
	case "invalid_arguments":
		label = "参数无效"
	case "path_not_authorized", "read_not_authorized", "tool_not_authorized", "grant_denied":
		label = "权限拒绝"
	case "environment_busy":
		label = "读取名额已满"
	case "read_failed":
		label = "读取失败"
	}
	if label == "" {
		return ""
	}
	return " · " + label
}

func withRunOutcome(lines []string, outcome sdk.RunOutcome, stop *sdk.StopDetail) []string {
	switch outcome {
	case sdk.BudgetExceeded:
		text := "Run · 预算已用尽"
		if stop != nil {
			dimension := map[sdk.StopCode]string{
				sdk.StopToolScanLimit: "扫描预算", sdk.StopToolRequestLimit: "工具次数",
				sdk.StopToolReceiptLimit: "回执字节", sdk.StopModelRequestLimit: "模型次数",
				sdk.StopModelInputLimit: "模型输入字节", sdk.StopModelOutputLimit: "共享输出字节",
			}[stop.Code]
			if dimension != "" {
				text += " · " + dimension
			}
		}
		return historyTail(lines, []string{text})
	case sdk.ToolOutcomeUnknown:
		text := "Run · 工具结果待核对"
		if stop != nil && stop.CallID != "" {
			text += " [" + toolText(string(stop.CallID), 64) + "]"
		}
		return historyTail(lines, []string{text})
	default:
		return lines
	}
}

func toolText(text string, limit int) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, prefix(text, limit))
}
