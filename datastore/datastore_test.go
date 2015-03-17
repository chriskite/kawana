package datastore

import (
	. "gopkg.in/check.v1"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DataStoreS struct{}

var _ = Suite(&DataStoreS{})

func checkForImpact(c *C, data IPData, amount ImpactAmount) {
	c.Check(data.MaxImpacts.FiveMin, Equals, amount)
	c.Check(data.MaxImpacts.Hour, Equals, amount)
	c.Check(data.MaxImpacts.Day, Equals, amount)
}

func (s *IPDataS) TestLogIP(c *C) {
	store := New("/tmp", "")
	ip := IPLong(0)
	amount := ImpactAmount(64)
	var data IPData

	// impact by amount
	data = store.LogIP(ip, amount, BWNop)

	checkForImpact(c, data, amount)

	// impact with amount and blackwhite modifiers
	data = store.LogIP(ip, amount, BWBlacklist)
	data = store.LogIP(ip, amount, BWWhitelist)

	checkForImpact(c, data, 3*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(3))
}

func (s *IPDataS) TestLogNewIPWAL(c *C) {
	store := New("/tmp", "")
	ip := IPLong(0)
	amount := ImpactAmount(64)
	var data IPData

	store.setWALStatus(walWriting)

	// impact by amount
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	store.setWALStatus(walDraining)

	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 2*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	// ip should be in the WAL
	c.Check(len(store.wal.getIPs()), Equals, 1)

	store.drainWAL()
	// WAL should now be empty
	c.Check(len(store.wal.getIPs()), Equals, 0)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 3*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	store.setWALStatus(walInactive)

	// WAL should still be empty
	c.Check(len(store.wal.getIPs()), Equals, 0)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 4*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))
}

func (s *IPDataS) TestLogExistingIPWAL(c *C) {
	store := New("/tmp", "")
	ip := IPLong(0)
	amount := ImpactAmount(64)
	var data IPData

	// impact by amount
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	store.setWALStatus(walWriting)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 2*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	store.setWALStatus(walDraining)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 3*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	// ip should be in WAL
	c.Check(len(store.wal.getIPs()), Equals, 1)

	store.drainWAL()

	// WAL should now be empty
	c.Check(len(store.wal.getIPs()), Equals, 0)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 4*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))

	store.setWALStatus(walInactive)

	// WAL should still be empty
	c.Check(len(store.wal.getIPs()), Equals, 0)

	// impact
	data = store.LogIP(ip, amount, BWNop)
	checkForImpact(c, data, 5*amount)
	c.Check(data.Forgiven, Equals, ForgivenNum(0))
	c.Check(data.BlackWhite, Equals, byte(0))
}
