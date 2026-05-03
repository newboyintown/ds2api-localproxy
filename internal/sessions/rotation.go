package sessions

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ds2api/internal/auth"
	"ds2api/internal/config"
	"ds2api/internal/sse"
)

// RotateAndSummarizeLocked rotates the current session using the provided active RequestAuth slots.
func (m *Manager) RotateAndSummarizeLocked(ctx context.Context, state *SessionState, oldA *auth.RequestAuth, newA *auth.RequestAuth) error {
	if state.DeepSeekSessionID == "" {
		return fmt.Errorf("no active session to rotate")
	}

	config.Logger.Info("[sessions] starting rotation", "client_session_id", state.ClientSessionID, "old_ds_session_id", state.DeepSeekSessionID, "account_id", state.AccountID)

	// 1. Send summary request to OLD session
	summaryPrompt := "Hãy tóm tắt lại các key ở trên, và đặc biệt là vấn đề hiện tại đang nói tới."
	payload := map[string]any{
		"chat_session_id":   state.DeepSeekSessionID,
		"parent_message_id": nil,
		"prompt":            summaryPrompt,
		"ref_file_ids":      []any{},
		"thinking_enabled":  false,
		"search_enabled":    false,
	}

	pow, err := m.ds.GetPow(ctx, oldA, 3)
	if err != nil {
		return fmt.Errorf("failed to get PoW for summary: %w", err)
	}

	resp, err := m.ds.CallCompletion(ctx, oldA, payload, pow, 3)
	if err != nil {
		return fmt.Errorf("failed to get summary completion: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("summary failed with status %d: %s", resp.StatusCode, string(body))
	}

	result := sse.CollectStream(resp, false, true)
	summaryText := strings.TrimSpace(result.Text)
	if summaryText == "" {
		summaryText = "Tóm tắt thất bại, hoặc nội dung trống."
	}

	config.Logger.Info("[sessions] obtained summary", "client_session_id", state.ClientSessionID)

	// 2. Create new session on NEW account
	newDsSessionID, err := m.ds.CreateSession(ctx, newA, 3)
	if err != nil {
		return fmt.Errorf("failed to create new session: %w", err)
	}

	config.Logger.Info("[sessions] acquired new session", "client_session_id", state.ClientSessionID, "new_ds_session_id", newDsSessionID, "new_account_id", newA.AccountID)

	// 3. Send summary context to NEW session
	injectionPrompt := "Đây là tóm tắt nội dung chat trước đó để tiếp tục context:\n\n" + summaryText + "\n\nHãy sẵn sàng để trả lời câu hỏi tiếp theo dựa trên ngữ cảnh này."
	injPayload := map[string]any{
		"chat_session_id":   newDsSessionID,
		"parent_message_id": nil,
		"prompt":            injectionPrompt,
		"ref_file_ids":      []any{},
		"thinking_enabled":  false,
		"search_enabled":    false,
	}

	powInj, err := m.ds.GetPow(ctx, newA, 3)
	if err == nil {
		respInj, errInj := m.ds.CallCompletion(ctx, newA, injPayload, powInj, 3)
		if errInj == nil {
			_ = sse.CollectStream(respInj, false, true)
			respInj.Body.Close()
		}
	}

	config.Logger.Info("[sessions] injected summary to new session", "client_session_id", state.ClientSessionID)

	// 4. Delete old session asynchronously
	oldToken := oldA.DeepSeekToken
	oldSessionID := state.DeepSeekSessionID
	go func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = m.ds.DeleteSessionForToken(delCtx, oldToken, oldSessionID)
	}()

	// 5. Update internal state
	state.DeepSeekSessionID = newDsSessionID
	state.AccountID = newA.AccountID
	state.DeepSeekToken = newA.DeepSeekToken
	state.TurnCount = 0
	state.LastUsed = time.Now()

	config.Logger.Info("[sessions] rotation complete", "client_session_id", state.ClientSessionID)
	return nil
}
