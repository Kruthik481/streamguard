// Package domain holds the types shared by the generator, rules, scorer and store.
package domain

import "time"

type Tx struct {
	ID          string    `json:"id"`
	CardID      string    `json:"card_id"`
	AmountCents int64     `json:"amount_cents"`
	MerchantID  string    `json:"merchant_id"`
	Country     string    `json:"country"`
	EventTime   time.Time `json:"event_time"`
}

// Before orders by event time, then id, so equal timestamps still sort deterministically.
func (t Tx) Before(o Tx) bool {
	if !t.EventTime.Equal(o.EventTime) {
		return t.EventTime.Before(o.EventTime)
	}
	return t.ID < o.ID
}

type Hit struct {
	Rule   string `json:"rule"`
	Weight int    `json:"weight"`
	Detail string `json:"detail"`
}

// Meta says where a transaction was scored.
type Meta struct {
	Partition int
	Worker    int
}

func Score(hits []Hit) int {
	total := 0
	for _, h := range hits {
		total += h.Weight
	}
	return total
}

func Severity(score int) string {
	switch {
	case score >= 5:
		return "high"
	case score >= 3:
		return "medium"
	case score >= 1:
		return "low"
	}
	return ""
}
