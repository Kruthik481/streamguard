// Package rules scores one transaction against its card's recent history. Everything here is pure.
package rules

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

const (
	Velocity              = "velocity"
	CardTesting           = "card_testing"
	ImpossibleTravel      = "impossible_travel"
	AmountSpike           = "amount_spike"
	NewMerchantHighAmount = "new_merchant_high_amount"

	VelocityCount   = 5
	VelocityWindow  = 60 * time.Second
	TestingCount    = 3
	TestingWindow   = 120 * time.Second
	TestingMaxCents = 200
	MaxSpeedKmh     = 900.0
	SpikeFactor     = 5.0
	SpikeMinHistory = 5
	HighAmountCents = 50_000
)

type rule func(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool)

var all = []rule{velocity, cardTesting, impossibleTravel, amountSpike, newMerchantHighAmount}

// Evaluate runs every rule. window may contain tx itself and later transactions; only strictly
// earlier ones (by event time, then id) count as history, which keeps results independent of the
// order in which redeliveries arrive.
func Evaluate(tx domain.Tx, window []domain.Tx) []domain.Hit {
	prior := make([]domain.Tx, 0, len(window))
	for _, e := range window {
		if e.ID != tx.ID && e.Before(tx) {
			prior = append(prior, e)
		}
	}
	slices.SortFunc(prior, func(a, b domain.Tx) int {
		if a.Before(b) {
			return -1
		}
		if b.Before(a) {
			return 1
		}
		return 0
	})
	var hits []domain.Hit
	for _, r := range all {
		if h, ok := r(tx, prior); ok {
			hits = append(hits, h)
		}
	}
	return hits
}

func countWithin(tx domain.Tx, prior []domain.Tx, d time.Duration, match func(domain.Tx) bool) int {
	n := 1 // tx itself
	for _, e := range prior {
		if tx.EventTime.Sub(e.EventTime) <= d && match(e) {
			n++
		}
	}
	return n
}

func velocity(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool) {
	n := countWithin(tx, prior, VelocityWindow, func(domain.Tx) bool { return true })
	return domain.Hit{Rule: Velocity, Weight: 2, Detail: fmt.Sprintf("%d transactions in 60s", n)}, n >= VelocityCount
}

func cardTesting(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool) {
	small := func(e domain.Tx) bool { return e.AmountCents < TestingMaxCents }
	if !small(tx) {
		return domain.Hit{}, false
	}
	n := countWithin(tx, prior, TestingWindow, small)
	return domain.Hit{Rule: CardTesting, Weight: 3, Detail: fmt.Sprintf("%d charges under $2 in 2 min", n)}, n >= TestingCount
}

func impossibleTravel(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool) {
	if len(prior) == 0 {
		return domain.Hit{}, false
	}
	prev := prior[len(prior)-1]
	if prev.Country == tx.Country {
		return domain.Hit{}, false
	}
	km := distanceKm(prev.Country, tx.Country)
	if math.IsNaN(km) {
		return domain.Hit{}, false
	}
	hours := tx.EventTime.Sub(prev.EventTime).Hours()
	speed := math.Inf(1)
	if hours > 0 {
		speed = km / hours
	}
	detail := fmt.Sprintf("%s → %s implies %.0f km/h", prev.Country, tx.Country, speed)
	return domain.Hit{Rule: ImpossibleTravel, Weight: 3, Detail: detail}, speed > MaxSpeedKmh
}

func amountSpike(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool) {
	if len(prior) < SpikeMinHistory {
		return domain.Hit{}, false
	}
	amounts := make([]int64, len(prior))
	for i, e := range prior {
		amounts[i] = e.AmountCents
	}
	slices.Sort(amounts)
	mid := len(amounts) / 2
	median := float64(amounts[mid])
	if len(amounts)%2 == 0 {
		median = float64(amounts[mid-1]+amounts[mid]) / 2
	}
	detail := fmt.Sprintf("$%.2f vs median $%.2f", float64(tx.AmountCents)/100, median/100)
	return domain.Hit{Rule: AmountSpike, Weight: 2, Detail: detail}, median > 0 && float64(tx.AmountCents) > SpikeFactor*median
}

func newMerchantHighAmount(tx domain.Tx, prior []domain.Tx) (domain.Hit, bool) {
	if tx.AmountCents <= HighAmountCents {
		return domain.Hit{}, false
	}
	for _, e := range prior {
		if e.MerchantID == tx.MerchantID {
			return domain.Hit{}, false
		}
	}
	detail := fmt.Sprintf("$%.2f at unseen merchant %s", float64(tx.AmountCents)/100, tx.MerchantID)
	return domain.Hit{Rule: NewMerchantHighAmount, Weight: 1, Detail: detail}, true
}
