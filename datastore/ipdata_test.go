package datastore

import (
	. "gopkg.in/check.v1"
	"math"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type IPDataS struct{}

var _ = Suite(&IPDataS{})

func (s *IPDataS) TestImpact(c *C) {
	d := new(IPData)
	c.Assert(*d, Equals, IPData{})

	amount := ImpactAmount(42)
	d.impact(amount, BWNop)

	// all time window max impacts should be set to the amount we impacted
	c.Check(d.MaxImpacts.FiveMin, Equals, amount)
	c.Check(d.MaxImpacts.Hour, Equals, amount)
	c.Check(d.MaxImpacts.Day, Equals, amount)
	// forgiven should remain 0
	c.Check(d.Forgiven, Equals, ForgivenNum(0))
	// BlackWhite should remain 0
	c.Check(d.BlackWhite, Equals, byte(0))
}

func (s *IPDataS) TestImpactBlackWhite(c *C) {
	d := new(IPData)

	amount := ImpactAmount(0)

	// whitelist is 0x01, blacklist is 0x02

	d.impact(amount, BWWhitelist)
	c.Check(d.BlackWhite, Equals, byte(1))
	d.impact(amount, BWBlacklist)
	c.Check(d.BlackWhite, Equals, byte(3))

	d.impact(amount, BWUnWhitelist)
	c.Check(d.BlackWhite, Equals, byte(2))
	d.impact(amount, BWUnBlacklist)
	c.Check(d.BlackWhite, Equals, byte(0))
}

func (s *IPDataS) TestImpactAtTIme(c *C) {
	d := new(IPData)
	amount := ImpactAmount(42)

	when := time.Now()
	d.impactAtTime(amount, BWNop, when)

	// simulate 5 minutes passing
	when = when.Add(5 * time.Minute)
	d.impactAtTime(amount, BWNop, when)

	c.Check(d.MaxImpacts.FiveMin, Equals, amount)
	c.Check(d.MaxImpacts.Hour, Equals, 2*amount)
	c.Check(d.MaxImpacts.Day, Equals, 2*amount)

	// simulate 1 hour passing
	when = when.Add(time.Hour)
	d.impactAtTime(amount, BWNop, when)
	c.Check(d.MaxImpacts.FiveMin, Equals, amount)
	c.Check(d.MaxImpacts.Hour, Equals, 2*amount)
	c.Check(d.MaxImpacts.Day, Equals, 3*amount)
}

func (s *IPDataS) TestForgive(c *C) {
	amount := ImpactAmount(100)

	d := IPData{
		MaxImpacts: ImpactAmounts{
			FiveMin: amount,
			Hour:    amount,
			Day:     amount,
		},
	}

	forg := amount - ImpactAmount(1)
	forgiveAmounts := ImpactAmounts{
		FiveMin: forg,
		Hour:    forg,
		Day:     forg,
	}

	d.forgive(forgiveAmounts)

	// all time window max impacts should be reduced
	c.Check(d.MaxImpacts.FiveMin, Equals, amount-forg)
	c.Check(d.MaxImpacts.Hour, Equals, amount-forg)
	c.Check(d.MaxImpacts.Day, Equals, amount-forg)
	// forgiven should be incremented
	c.Check(d.Forgiven, Equals, ForgivenNum(1))
}

func (s *IPDataS) TestAdd(c *C) {
	a := ImpactAmount(2)
	b := ImpactAmount(2)
	c.Check(a.add(b), Equals, ImpactAmount(4))
}

func (s *IPDataS) TestAddDoesNotOverflow(c *C) {
	a := ImpactAmount(math.MaxUint32 - 1)
	b := ImpactAmount(2)
	c.Check(a.add(b), Equals, ImpactAmount(math.MaxUint32))
}

func (s *IPDataS) TestSub(c *C) {
	a := ImpactAmount(4)
	b := ImpactAmount(2)
	c.Check(a.sub(b), Equals, ImpactAmount(2))
}

func (s *IPDataS) TestSubDoesNotUnderflow(c *C) {
	a := ImpactAmount(1)
	b := ImpactAmount(2)
	c.Check(a.sub(b), Equals, ImpactAmount(0))
}

func (s *IPDataS) TestMax(c *C) {
	a := ImpactAmount(1)
	b := ImpactAmount(2)
	c.Check(max(a, b), Equals, b)
}
