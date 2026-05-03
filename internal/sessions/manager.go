package sessions

import (
	"context"
	"sync"
	"time"

	"ds2api/internal/account"

	"ds2api/internal/httpapi/openai/shared"
)

const TurnLimit = 25

type SessionState struct {
	ClientSessionID   string
	DeepSeekSessionID string
	AccountID         string
	DeepSeekToken     string
	TurnCount         int
	LastUsed          time.Time
	mu                sync.Mutex
}

type Manager struct {
	mu       sync.Mutex
	sessions map[string]*SessionState
	pool     *account.Pool
	auth     shared.AuthResolver
	ds       shared.DeepSeekCaller
}

var globalManager *Manager

func InitManager(pool *account.Pool, authResolver shared.AuthResolver, ds shared.DeepSeekCaller) {
	globalManager = &Manager{
		sessions: make(map[string]*SessionState),
		pool:     pool,
		auth:     authResolver,
		ds:       ds,
	}
	globalManager.startCleanupRoutine()
}

func GlobalManager() *Manager {
	return globalManager
}

func (m *Manager) startCleanupRoutine() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			m.mu.Lock()
			now := time.Now()
			for id, session := range m.sessions {
				if now.Sub(session.LastUsed) > 2*time.Hour {
					if session.DeepSeekSessionID != "" && session.DeepSeekToken != "" {
						delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						_, _ = m.ds.DeleteSessionForToken(delCtx, session.DeepSeekToken, session.DeepSeekSessionID)
						cancel()
					}
					delete(m.sessions, id)
				}
			}
			m.mu.Unlock()
		}
	}()
}

func (m *Manager) GetSession(clientSessionID string) *SessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[clientSessionID]; ok {
		s.LastUsed = time.Now()
		return s
	}
	s := &SessionState{
		ClientSessionID: clientSessionID,
		LastUsed:        time.Now(),
	}
	m.sessions[clientSessionID] = s
	return s
}

func (m *Manager) RemoveSession(clientSessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, clientSessionID)
}

func (s *SessionState) Lock() {
	s.mu.Lock()
}

func (s *SessionState) Unlock() {
	s.mu.Unlock()
}
