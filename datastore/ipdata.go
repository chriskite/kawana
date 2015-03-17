package datastore

import (
	"errors"
	"log"
	"math"
	"sync"
	"time"
)

// IPLong is an ipv4 address as a uint32
type IPLong uint32

// ImpactAmount is an integer representing the impact of an IP addr
// over a particular time window
type ImpactAmount uint32

// ForgivenNum is an integer number of times an IP addr has been forgiven
type ForgivenNum uint16

// ImpactAmounts is a struct of 3 time windows' impacts
type ImpactAmounts struct {
	FiveMin, Hour, Day ImpactAmount
}

// StartTimes is a struct of 3 time window start times
type StartTimes struct {
	FiveMin, Hour, Day uint32
}

// IPData holds impact amounts, time window starts, forgiveness,
// and white/black list info for an IP address
type IPData struct {
	Mutex      sync.RWMutex
	CurImpacts ImpactAmounts
	MaxImpacts ImpactAmounts
	StartTimes StartTimes
	Forgiven   ForgivenNum
	BlackWhite byte
}

// IPDataMap is a map from IPLong to *IPData
type IPDataMap map[IPLong]*IPData

func blackWhiteIPData(data *IPData, blackWhite BWModifier) error {
	switch blackWhite {
	case BWWhitelist:
		data.BlackWhite |= byte(0x01)
		break
	case BWUnWhitelist:
		data.BlackWhite &= byte(0xFE)
		break
	case BWBlacklist:
		data.BlackWhite |= byte(0x02)
		break
	case BWUnBlacklist:
		data.BlackWhite &= byte(0xFD)
		break
	default:
		return errors.New("Unknown BlackWhite modifier")
	}

	return nil
}

// impactIPData updates the IPData arg in place by adding the impact to the time windows.
//
// Takes a write lock on the IPData
func impactIPData(data *IPData, impact ImpactAmount, blackWhite BWModifier) {
	data.Mutex.Lock()
	defer data.Mutex.Unlock()

	if blackWhite != BWNop {
		err := blackWhiteIPData(data, blackWhite)
		if err != nil {
			log.Println(err)
		}
	}

	if impact == 0 {
		return
	}

	// five minute window
	if time.Now().After(time.Unix(int64(data.StartTimes.FiveMin), 0).Add(5 * time.Minute)) {
		data.StartTimes.FiveMin = uint32(time.Now().Unix())
		data.CurImpacts.FiveMin = impact
	} else {
		data.CurImpacts.FiveMin = data.CurImpacts.FiveMin.add(impact)
	}
	data.MaxImpacts.FiveMin = max(data.CurImpacts.FiveMin, data.MaxImpacts.FiveMin)

	// 1 hour window
	if time.Now().After(time.Unix(int64(data.StartTimes.Hour), 0).Add(time.Hour)) {
		data.StartTimes.Hour = uint32(time.Now().Unix())
		data.CurImpacts.Hour = impact
	} else {
		data.CurImpacts.Hour = data.CurImpacts.Hour.add(impact)
	}
	data.MaxImpacts.Hour = max(data.CurImpacts.Hour, data.MaxImpacts.Hour)

	// 1 day window
	if time.Now().After(time.Unix(int64(data.StartTimes.Day), 0).Add(24 * time.Hour)) {
		data.StartTimes.Day = uint32(time.Now().Unix())
		data.CurImpacts.Day = impact
	} else {
		data.CurImpacts.Day = data.CurImpacts.Day.add(impact)
	}
	data.MaxImpacts.Day = max(data.CurImpacts.Day, data.MaxImpacts.Day)
}

// forgive subtracts the given amounts from all the IPData's impact amounts
func (data *IPData) forgive(impacts ImpactAmounts) {
	data.MaxImpacts.FiveMin = data.MaxImpacts.FiveMin.sub(impacts.FiveMin)
	data.CurImpacts.FiveMin = data.MaxImpacts.FiveMin

	data.MaxImpacts.Hour = data.MaxImpacts.Hour.sub(impacts.Hour)
	data.CurImpacts.Hour = data.MaxImpacts.Hour

	data.MaxImpacts.Day = data.MaxImpacts.Day.sub(impacts.Day)
	data.CurImpacts.Day = data.MaxImpacts.Day

	data.Forgiven++
}

// add adds 2 impact amounts and returns the result.
// If the result would be larger than a uint32, it instead returns MaxUint32
func (a ImpactAmount) add(b ImpactAmount) ImpactAmount {
	sum := uint64(a) + uint64(b)
	if sum > uint64(math.MaxUint32) {
		return math.MaxUint32
	}
	return ImpactAmount(sum)
}

// sub subtracts 2 impact amounts and returns the result.
// If the result would be negative, it instead returns 0
func (a ImpactAmount) sub(b ImpactAmount) ImpactAmount {
	if b >= a {
		return 0
	}
	return a - b
}

// max returns the larger of 2 impact amounts
func max(a, b ImpactAmount) ImpactAmount {
	if a > b {
		return a
	}
	return b
}
