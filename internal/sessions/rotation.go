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

// RotateAndSummarize rotates the current session by asking for a summary,
// acquiring a new account slot, creating a new session, and deleting the old one.
func (m *Manager) RotateAndSummarize(ctx context.Context, state *SessionState, req *http.Request) error {
	if state.DeepSeekSessionID == "" || state.Auth == nil {
		return fmt.Errorf("no active session to rotate")
	}

	config.Logger.Info("[sessions] starting rotation", "client_session_id", state.ClientSessionID, "old_ds_session_id", state.DeepSeekSessionID, "account_id", state.AccountID)

	// 1. Send summary request
	summaryPrompt := "Hãy tóm tắt lại các key ở trên, và đặc biệt là vấn đề hiện tại đang nói tới."
	payload := map[string]any{
		"chat_session_id":   state.DeepSeekSessionID,
		"parent_message_id": nil,
		"prompt":            summaryPrompt,
		"ref_file_ids":      []any{},
		"thinking_enabled":  false,
		"search_enabled":    false,
	}

	pow, err := m.ds.GetPow(ctx, state.Auth, 3)
	if err != nil {
		return fmt.Errorf("failed to get PoW for summary: %w", err)
	}

	resp, err := m.ds.CallCompletion(ctx, state.Auth, payload, pow, 3)
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

	// 2. Acquire new account and create new session
	var newAuth *auth.RequestAuth
	var newDsSessionID string

	dummyReq := req.Clone(ctx)
	dummyReq.Header.Del("X-Ds2-Target-Account")

	maxAccountRetries := 5
	for i := 0; i < maxAccountRetries; i++ {
		a, err := m.auth.Determine(dummyReq)
		if err != nil {
			return fmt.Errorf("failed to acquire new account for rotation: %w", err)
		}

		newSessionID, err := m.ds.CreateSession(ctx, a, 3)
		if err != nil {
			m.auth.Release(a)
			time.Sleep(1 * time.Second)
			continue
		}
		newAuth = a
		newDsSessionID = newSessionID
		break
	}
	if newAuth == nil {
	    return fmt.Errorf("failed to acquire new account for rotation after retries")
	}

	config.Logger.Info("[sessions] acquired new session", "client_session_id", state.ClientSessionID, "new_ds_session_id", newDsSessionID, "new_account_id", newAuth.AccountID)

	// 3. Send summary to new session
	injectionPrompt := "Đây là tóm tắt nội dung chat trước đó để tiếp tục context:\n\n" + summaryText + "\n\nHãy sẵn sàng để trả lời câu hỏi tiếp theo dựa trên ngữ cảnh này."
	injPayload := map[string]any{
		"chat_session_id":   newDsSessionID,
		"parent_message_id": nil,
		"prompt":            injectionPrompt,
		"ref_file_ids":      []any{},
		"thinking_enabled":  false,
		"search_enabled":    false,
	}

	powInj, err := m.ds.GetPow(ctx, newAuth, 3)
	if err == nil {
		respInj, errInj := m.ds.CallCompletion(ctx, newAuth, injPayload, powInj, 3)
		if errInj == nil {
			_ = sse.CollectStream(respInj, false, true)
			respInj.Body.Close()
		}
	}

	config.Logger.Info("[sessions] injected summary to new session", "client_session_id", state.ClientSessionID)

	// 4. Delete old session
	oldAuth := state.Auth
	oldSessionID := state.DeepSeekSessionID
	go func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = m.ds.DeleteSessionForToken(delCtx, oldAuth.DeepSeekToken, oldSessionID)
		m.auth.Release(oldAuth) // release the old slot
	}()

	// 5. Update state
	state.DeepSeekSessionID = newDsSessionID
	state.AccountID = newAuth.AccountID
	state.Auth = newAuth
	state.TurnCount = 0

	config.Logger.Info("[sessions] rotation complete", "client_session_id", state.ClientSessionID)
	return nil
}
