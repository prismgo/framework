package rabbitmq

import "time"

func (c *Connection) setTopologyCacheNowForTest(now func() time.Time) {
	c.topologyNow = now
}
