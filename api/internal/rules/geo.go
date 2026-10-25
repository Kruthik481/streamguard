package rules

import (
	"math"
	"slices"
)

// centroids are approximate country centres (lat, lon) for the countries the generator uses.
var centroids = map[string][2]float64{
	"US": {39.8, -98.6}, "CA": {56.1, -106.3}, "MX": {23.6, -102.6}, "BR": {-14.2, -51.9},
	"AR": {-38.4, -63.6}, "GB": {55.4, -3.4}, "FR": {46.2, 2.2}, "DE": {51.2, 10.5},
	"ES": {40.5, -3.7}, "IT": {41.9, 12.6}, "NL": {52.1, 5.3}, "SE": {60.1, 18.6},
	"PL": {51.9, 19.1}, "TR": {38.9, 35.2}, "EG": {26.8, 30.8}, "NG": {9.1, 8.7},
	"ZA": {-30.6, 22.9}, "KE": {0.0, 37.9}, "AE": {23.4, 53.8}, "SA": {23.9, 45.1},
	"IN": {20.6, 79.0}, "PK": {30.4, 69.3}, "BD": {23.7, 90.4}, "CN": {35.9, 104.2},
	"JP": {36.2, 138.3}, "KR": {35.9, 127.8}, "SG": {1.35, 103.8}, "ID": {-0.8, 113.9},
	"AU": {-25.3, 133.8}, "NZ": {-40.9, 174.9},
}

func Countries() []string {
	out := make([]string, 0, len(centroids))
	for c := range centroids {
		out = append(out, c)
	}
	slices.Sort(out)
	return out
}

// distanceKm is the great-circle distance between two country centroids, or NaN if either is unknown.
func distanceKm(a, b string) float64 {
	pa, okA := centroids[a]
	pb, okB := centroids[b]
	if !okA || !okB {
		return math.NaN()
	}
	const earthRadiusKm = 6371.0
	rad := math.Pi / 180
	dLat := (pb[0] - pa[0]) * rad
	dLon := (pb[1] - pa[1]) * rad
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(pa[0]*rad)*math.Cos(pb[0]*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(h))
}
