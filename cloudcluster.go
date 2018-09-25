package radix

import (
	"net"
	"time"
)

// CloudCluster extends Cluster with DNS refresh mechanism
type CloudCluster struct {
	*Cluster

	initialClusterAddrs []string // can store DNS entries

}

// NewCloudCluster initializes and returns a Cluster instance. It will try every
// address given until it finds a usable one. From there it uses CLUSTER SLOTS
// to discover the cluster topology and make all the necessary connections.
//
// NewCluster takes in a number of options which can overwrite its default
// behavior. The default options NewCluster uses are:
//
//	ClusterPoolFunc(DefaultClientFunc)
//
func NewCloudCluster(clusterAddrs []string, opts ...ClusterOpt) (*CloudCluster, error) {
	baseCluster, err := NewCluster(clusterAddrs, opts...)
	if err != nil {
		return nil, err
	}

	c := &CloudCluster{
		Cluster:             baseCluster,
		initialClusterAddrs: clusterAddrs,
	}

	// forward bundled error logging
	go func() {
		select {
		case err := <-c.Cluster.ErrCh:
			c.co.log.Logf(2, "Radix error : %s", err)
		}
	}()

	// replace creation of pools with the clever one that goes through DNS reading
	c.updatePoolsFromDNS()

	// ao
	c.updateClusterPoolFromDNSEvery(10 * time.Second)

	return c, nil
}

// ao : DNS handling
// -------------------------------------------

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (c *CloudCluster) AddrForKey(key string) string {
	return c.addrForKey(key)
}

func (c *CloudCluster) resolveClusterHostPorts() []string {
	smap := make(map[string]struct{})
	for _, addr := range c.initialClusterAddrs {

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			c.co.log.Warnf("Bad host:port %s", addr)
			continue
		}

		ips, err := net.LookupIP(host)
		if err != nil {
			c.co.log.Warnf("Error while resolving %s : %s", addr, err)
			continue
		}
		ipl := ""
		for _, ip := range ips {
			ipport := net.JoinHostPort(ip.String(), port)
			smap[ipport] = struct{}{}
			ipl = ipl + ipport + ", "
		}
		c.co.log.Logf(4, "Resolved %s : %s", addr, ipl)
	}
	out := []string{}
	for k := range smap {
		out = append(out, k)
	}
	c.co.log.Logf(4, "deduped Resolved ips:ports : %s", out)
	return out
}

// this will attempt a connection to an IP and add it to the cluster pool if
// connection is successful
// this is threadsafe
func (c *CloudCluster) safeCreatePoolForAddr(addr string) error {
	// check if there is already a connection in the pool before creating new one
	c.l.Lock()
	_, ok := c.pools[addr]
	c.l.Unlock()
	if ok {
		return nil
	}

	// it's important that the cluster pool set isn't locked while this is
	// happening, because this could block for a while
	p, err := c.co.pf("tcp", addr)
	if err != nil {
		return err
	}

	// we've made a new pool, but we need to double-check someone else didn't
	// make one at the same time and add it in first. If they did, close this
	// one and return that one
	c.l.Lock()
	if _, ok := c.pools[addr]; ok {
		c.l.Unlock()
		p.Close()
		return nil
	}
	c.pools[addr] = p
	c.l.Unlock()
	return nil
}

func (c *CloudCluster) debugPoolHosts() {
	c.l.RLock()
	addrs := make([]string, 0, len(c.pools))
	for addr := range c.pools {
		addrs = append(addrs, addr)
	}
	c.l.RUnlock()
	c.co.log.Logf(4, "pool hosts : %s", addrs)
}

func (c *CloudCluster) updatePoolsFromDNS() error {

	ips := c.resolveClusterHostPorts()

	c.debugPoolHosts()

	// retrieve the pools
	c.l.RLock()
	addrs := make([]string, 0, len(c.pools))
	for addr := range c.pools {
		addrs = append(addrs, addr)
	}
	c.l.RUnlock()

	// decide if pools needs to be created.
	poolHasIp := false

	// there are 2 possibilities here.
	// either the DNS returns all the possible IPs of pods (>1), then
	// the IPs contain master and slaves. since the pool contains only masters,
	// then check if at least one IP from the pool is in DNS
	if len(ips) > 1 {
		for _, addr := range ips {
			if contains(addrs, addr) {
				poolHasIp = true
			}
		}
	} else {
		// here only returned 1 IP. either because the openshift service is not clusterIP:None
		// or because that's simply not an openshift service
		poolHasIp = len(addrs) > 0
	}

	if !poolHasIp {
		c.co.log.Logf(4, "pool missing IPs from DNS. creating pools")
		for _, addr := range ips {
			// todo : parallelize running in goroutine
			c.co.log.Logf(2, "Creating DNS pool for addr %s", addr)
			c.safeCreatePoolForAddr(addr)
		}
	} else {
		c.co.log.Logf(4, "pool has at least one IPs from DNS. nothing to do")
	}

	return nil
}

func (c *CloudCluster) updateClusterPoolFromDNSEvery(d time.Duration) {
	c.closeWG.Add(1)
	go func() {
		defer c.closeWG.Done()
		t := time.NewTicker(d)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if err := c.updatePoolsFromDNS(); err != nil {
					c.err(err)
				}
			case <-c.closeCh:
				return
			}
		}
	}()
}
