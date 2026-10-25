package gen

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

const (
	CardTesting      = "card_testing"
	ImpossibleTravel = "impossible_travel"
	AccountTakeover  = "account_takeover"
)

var Scenarios = []string{CardTesting, ImpossibleTravel, AccountTakeover}

var ErrUnknownScenario = errors.New("unknown scenario")

// Scenario builds an attack on a fresh card. Event times are back-dated so the pattern exists
// the moment it is published.
func (g *Generator) Scenario(name string) ([]domain.Tx, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cardID := "scn-" + strings.ToLower(ulid.Make().String()[18:])
	mk := func(at time.Duration, cents int64, merchant, country string) domain.Tx {
		return domain.Tx{ID: ulid.Make().String(), CardID: cardID, AmountCents: cents,
			MerchantID: merchant, Country: country, EventTime: g.stamp(at)}
	}
	var out []domain.Tx
	switch name {
	case CardTesting: // six sub-$2 charges inside 40s
		for i := range 6 {
			out = append(out, mk(-40*time.Second+time.Duration(i)*8*time.Second, 50+g.rng.Int64N(101), "m-test", "US"))
		}
	case ImpossibleTravel: // India, then the US ten minutes later
		out = append(out, mk(-10*time.Minute, 4200, "m-in-1", "IN"), mk(0, 3900, "m-us-1", "US"))
	case AccountTakeover: // five ordinary purchases, then three large ones at new merchants
		for i := range 5 {
			out = append(out, mk(-50*time.Minute+time.Duration(i)*10*time.Minute, 5500+g.rng.Int64N(1000), fmt.Sprintf("m-home-%d", i%2), "GB"))
		}
		for i := range 3 {
			out = append(out, mk(-2*time.Minute+time.Duration(i)*time.Minute, 60000+g.rng.Int64N(10000), fmt.Sprintf("m-new-%d", i), "GB"))
		}
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownScenario, name)
	}
	return out, nil
}
