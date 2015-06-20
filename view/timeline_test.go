package view

import "testing"

func TestTimeline(t *testing.T) {
	// See utils_test for function.
	iter := buildIter(t)

	events, err := Timeline(iter, Ascending)

	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 6 {
		t.Errorf("expected 6 events, got %d", len(events))
	}
}
