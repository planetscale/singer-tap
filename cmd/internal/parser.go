package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
)

func ParseSavedState(stateFilePath string) (*State, error) {
	contents, err := ioutil.ReadFile(stateFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read file at path %v", stateFilePath)
	}

	return parseSavedStateContents(contents)
}

func parseSavedStateContents(contents []byte) (*State, error) {
	var (
		state        *State
		wrappedState *WrappedState
		err          error
	)

	state, err = ParseContents(contents, state)
	if err != nil {
		return nil, fmt.Errorf("state file contents are invalid: %q", err)
	}

	if state == nil || len(state.Streams) == 0 {
		wrappedState, err = ParseContents(contents, wrappedState)
		if err != nil {
			return nil, fmt.Errorf("state file contents are invalid: %q", err)
		}
		if wrappedState != nil {
			state = &wrappedState.Value
		}
	}

	return state, nil
}

func Parse[T any](path string, obj T) (T, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return obj, errors.Wrapf(err, "unable to read file at path %v", path)
	}

	return ParseContents(b, obj)
}

func ParseContents[T any](content []byte, obj T) (T, error) {
	if err := json.Unmarshal(content, &obj); err != nil {
		return obj, err
	}
	return obj, nil
}
