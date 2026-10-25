package domain

import (
	"testing"
	"time"
)

func TestBeforeOrdersByTimeThenID(t *testing.T) {
	t0 := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	a := Tx{ID: "b", EventTime: t0}
	b := Tx{ID: "a", EventTime: t0.Add(time.Millisecond)}
	c := Tx{ID: "c", EventTime: t0}
	if !a.Before(b) || b.Before(a) {
		t.Fatal("earlier event time must sort first")
	}
	if !a.Before(c) || c.Before(a) {
		t.Fatal("ties break on id")
	}
}

func TestScoreAndSeverity(t *testing.T) {
	cases := []struct {
		hits []Hit
		want string
	}{
		{nil, ""},
		{[]Hit{{Weight: 1}}, "low"},
		{[]Hit{{Weight: 2}}, "low"},
		{[]Hit{{Weight: 3}}, "medium"},
		{[]Hit{{Weight: 2}, {Weight: 2}}, "medium"},
		{[]Hit{{Weight: 3}, {Weight: 2}}, "high"},
	}
	for _, c := range cases {
		if got := Severity(Score(c.hits)); got != c.want {
			t.Errorf("hits %v: got %q want %q", c.hits, got, c.want)
		}
	}
}
