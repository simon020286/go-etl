package db

import (
	"database/sql"
	"sync"
)

// Manager provides centralized access to all database managers
type Manager struct {
	db                    *sql.DB
	pipelineManager       *PipelineManager
	pipelineStateManager  *PipelineStateManager
	executionManager      *ExecutionManager
	mu                    sync.RWMutex
}

var (
	managerInstance *Manager
	managerOnce     sync.Once
)

// GetManager returns a singleton manager instance
func GetManager() (*Manager, error) {
	var err error
	managerOnce.Do(func() {
		db, dbErr := GetDB()
		if dbErr != nil {
			err = dbErr
			return
		}
		managerInstance = &Manager{
			db: db,
		}
	})
	return managerInstance, err
}

// GetManagerWithDB creates a manager with a specific database connection
func GetManagerWithDB(db *sql.DB) *Manager {
	return &Manager{
		db: db,
	}
}

// Pipelines returns the pipeline manager
func (m *Manager) Pipelines() *PipelineManager {
	m.mu.RLock()
	if m.pipelineManager != nil {
		defer m.mu.RUnlock()
		return m.pipelineManager
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if m.pipelineManager == nil {
		m.pipelineManager = NewPipelineManager(m.db)
	}

	return m.pipelineManager
}

// PipelineState returns the pipeline state manager
func (m *Manager) PipelineState() *PipelineStateManager {
	m.mu.RLock()
	if m.pipelineStateManager != nil {
		defer m.mu.RUnlock()
		return m.pipelineStateManager
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if m.pipelineStateManager == nil {
		m.pipelineStateManager = NewPipelineStateManager(m.db, m.Pipelines())
	}

	return m.pipelineStateManager
}

// Executions returns the execution manager
func (m *Manager) Executions() *ExecutionManager {
	m.mu.RLock()
	if m.executionManager != nil {
		defer m.mu.RUnlock()
		return m.executionManager
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if m.executionManager == nil {
		m.executionManager = NewExecutionManager(m.db)
	}

	return m.executionManager
}

// DB returns the underlying database connection
func (m *Manager) DB() *sql.DB {
	return m.db
}

// Close closes the database connection
func (m *Manager) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// ResetManager resets the singleton manager (useful for testing)
func ResetManager() {
	managerOnce = sync.Once{}
	if managerInstance != nil {
		managerInstance.Close()
		managerInstance = nil
	}
}