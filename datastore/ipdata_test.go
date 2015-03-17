package datastore

import (
	. "gopkg.in/check.v1"
	"math"
	"testing"
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
	// forgiven should remain 0
	// BlackWhite should remain 0
	c.Check(d.MaxImpacts.FiveMin, Equals, amount)
	c.Check(d.MaxImpacts.Hour, Equals, amount)
	c.Check(d.MaxImpacts.Day, Equals, amount)
	c.Check(d.Forgiven, Equals, ForgivenNum(0))
	c.Check(d.BlackWhite, Equals, byte(0))
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
