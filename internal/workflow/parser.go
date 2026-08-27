package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrInvalidWorkflow = errors.New("invalid workflow definition")
	ErrCycleDetected   = errors.New("cycle detected in workflow tasks")
	ErrMissingTask     = errors.New("task dependency does not exist")
)

type TaskDef struct {
	ID        string   `json:"id"`
	Type      string   `json:"type"`
	DependsOn []string `json:"depends_on"`
}

type Definition struct {
	Name  string    `json:"name"`
	Tasks []TaskDef `json:"tasks"`
}

func Parse(data []byte) (*Definition, error) {
	var def Definition
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWorkflow, err)
	}

	if def.Name == "" {
		return nil, fmt.Errorf("%w: missing workflow name", ErrInvalidWorkflow)
	}
	if len(def.Tasks) == 0 {
		return nil, fmt.Errorf("%w: workflow has no tasks", ErrInvalidWorkflow)
	}

	return &def, ValidateDAG(&def)
}

func ValidateDAG(def *Definition) error {
	taskMap := make(map[string]TaskDef)
	for _, t := range def.Tasks {
		if t.ID == "" {
			return fmt.Errorf("%w: task missing ID", ErrInvalidWorkflow)
		}
		if _, exists := taskMap[t.ID]; exists {
			return fmt.Errorf("%w: duplicate task ID %s", ErrInvalidWorkflow, t.ID)
		}
		taskMap[t.ID] = t
	}

	// 1. Ensure all dependencies exist
	for _, t := range def.Tasks {
		for _, dep := range t.DependsOn {
			if _, exists := taskMap[dep]; !exists {
				return fmt.Errorf("%w: task %s depends on %s", ErrMissingTask, t.ID, dep)
			}
		}
	}

	// 2. Cycle detection using Kahn's algorithm or DFS
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var hasCycle func(taskID string) bool
	hasCycle = func(taskID string) bool {
		visited[taskID] = true
		recStack[taskID] = true

		for _, dep := range taskMap[taskID].DependsOn {
			if !visited[dep] {
				if hasCycle(dep) {
					return true
				}
			} else if recStack[dep] {
				return true
			}
		}

		recStack[taskID] = false
		return false
	}

	for id := range taskMap {
		if !visited[id] {
			if hasCycle(id) {
				return ErrCycleDetected
			}
		}
	}

	return nil
}
