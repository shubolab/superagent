package contextview

import (
	"encoding/json"
	"maps"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
)

type Block struct {
	Start     int
	Messages  []contract.Message
	Protected bool
}

func Blocks(messages []contract.Message, pins []int) []Block {
	var blocks []Block
	for start := 0; start < len(messages); {
		end := start + 1
		protected := messages[start].User != nil
		if assistant := messages[start].Assistant; assistant != nil {
			end += len(assistant.ToolCalls)
		}
		for _, pin := range pins {
			protected = protected || (pin >= start && pin < end)
		}
		blocks = append(blocks, Block{Start: start, Messages: messages[start:end], Protected: protected || end == len(messages)})
		start = end
	}
	return blocks
}

func Thresholds(limit uint64, p contract.ContextCompactionPolicy) (uint64, uint64) {
	// Floor against the supplied ratio. A very small window can trigger at zero.
	return uint64(math.Floor(float64(limit) * p.TriggerRatio)), uint64(math.Floor(float64(limit) * p.TargetRatio))
}

func ValidPolicy(p contract.ContextCompactionPolicy) bool {
	return p.TargetRatio > 0 && p.TargetRatio < p.TriggerRatio && p.TriggerRatio < 1 &&
		p.SummaryMaxTokens > 0 && p.SummaryMaxBytes > 0 && p.MaxCompactionsPerRun > 0 &&
		p.SourcePageBytes > 0 && p.SourceTool != ""
}

type View struct {
	Hidden    map[int]bool
	Shortened map[int]contract.ContextSourceRef
	Summary   []contract.ContextSummaryEntry
}

func (v View) Copy() View {
	out := View{
		Hidden:    make(map[int]bool, len(v.Hidden)),
		Shortened: make(map[int]contract.ContextSourceRef, len(v.Shortened)),
		Summary:   slices.Clone(v.Summary),
	}
	maps.Copy(out.Hidden, v.Hidden)
	maps.Copy(out.Shortened, v.Shortened)
	return out
}

func (v View) Messages(blocks []Block) []contract.Message {
	var messages []contract.Message
	for _, b := range blocks {
		switch {
		case !b.Protected && v.Hidden[b.Start]:
		case !b.Protected && v.Shortened[b.Start] != "":
			for _, m := range b.Messages {
				if m.Tool != nil && m.Tool.Result.Completion != nil && m.Tool.Result.Completion.Content != "" {
					tool, completion := *m.Tool, *m.Tool.Result.Completion
					ref, _ := json.Marshal(v.Shortened[b.Start])
					short := `{"context_source":` + string(ref) + `}`
					if len(short) < len(completion.Content) {
						completion.Content = short
						tool.Result.Completion = &completion
						m.Tool = &tool
					}
				}
				messages = append(messages, m)
			}
		default:
			messages = append(messages, b.Messages...)
		}
	}
	return messages
}

func (v View) References() []contract.ContextSourceRef {
	refs := slices.Collect(maps.Values(v.Shortened))
	for _, item := range v.Summary {
		refs = append(refs, item.Sources...)
	}
	return refs
}

type SummarySource struct {
	Ref      contract.ContextSourceRef
	Original string
}

func AddSummary(base View, generated []contract.ContextSummaryEntry, allowed map[contract.ContextSourceRef]int) (View, bool) {
	if len(generated) == 0 {
		return View{}, false
	}
	view := base.Copy()
	for _, entry := range generated {
		switch entry.Section {
		case "goal_constraints", "decisions", "effects", "pending":
		default:
			return View{}, false
		}
		if entry.Text == "" || !utf8.ValidString(entry.Text) || len(entry.Sources) == 0 {
			return View{}, false
		}
		for _, ref := range entry.Sources {
			block, ok := allowed[ref]
			if !ok {
				return View{}, false
			}
			view.Hidden[block] = true
			delete(view.Shortened, block)
		}
	}
	view.Summary = append(view.Summary, generated...)
	return view, true
}
