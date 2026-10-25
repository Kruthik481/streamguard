package hub

import "testing"

func TestSubscribersReceiveEvents(t *testing.T) {
	h := New(4)
	a, cancelA := h.Subscribe()
	b, cancelB := h.Subscribe()
	defer cancelA()
	defer cancelB()
	h.Publish(Event{Type: "scored", Data: 1})
	if (<-a).Type != "scored" || (<-b).Type != "scored" {
		t.Fatal("both subscribers must receive the event")
	}
}

func TestSlowSubscriberIsDropped(t *testing.T) {
	h := New(2)
	slow, cancel := h.Subscribe()
	defer cancel()
	for i := range 3 {
		h.Publish(Event{Type: "scored", Data: i})
	}
	n := 0
	for range slow {
		n++
	}
	if n != 2 || h.Subscribers() != 0 {
		t.Fatalf("received %d, subscribers %d", n, h.Subscribers())
	}
}

func TestCancelAndCloseAreSafe(t *testing.T) {
	h := New(1)
	ch, cancel := h.Subscribe()
	cancel()
	cancel() // second cancel is a no-op
	if _, ok := <-ch; ok {
		t.Fatal("channel must be closed after cancel")
	}
	other, _ := h.Subscribe()
	h.Close()
	if _, ok := <-other; ok {
		t.Fatal("Close must close every subscriber")
	}
	late, _ := h.Subscribe()
	if _, ok := <-late; ok {
		t.Fatal("subscribing after Close returns a closed channel")
	}
	h.Publish(Event{Type: "ignored"}) // must not panic
}
