package sessions

import (
	"strings"

	"ds2api/internal/promptcompat"
)

// ExtractLatestUserMessage extracts the prompt content of only the *last* user message
func ExtractLatestUserMessage(messages []any) string {
	if len(messages) == 0 {
		return ""
	}

	for i := len(messages) - 1; i >= 0; i-- {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(asString(msg["role"])))
		if role == "developer" {
		    role = "system"
		}
		if role == "user" || role == "system" {
			return strings.TrimSpace(promptcompat.NormalizeOpenAIContentForPrompt(msg["content"]))
		}
	}

	return ""
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
