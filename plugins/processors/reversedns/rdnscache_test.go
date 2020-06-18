package reversedns

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleReverseDNSLookup(t *testing.T) {
	d := NewReverseDNSCache(1*time.Second, 1*time.Second, -1)
	answer := d.Lookup("8.8.8.8")
	require.Equal(t, []string{"dns.google."}, answer)
	d.waitForWorkers()

	// do another request with no workers available.
	// it should read from cache instantly.
	answer = d.Lookup("8.8.8.8")
	require.Equal(t, []string{"dns.google."}, answer)

	require.Len(t, d.cache, 1)
	require.Len(t, d.expireList, 1)
	d.cleanup()
	require.Len(t, d.expireList, 1) // ttl hasn't hit yet.

	stats := d.Stats()

	require.EqualValues(t, 0, stats.CacheExpire)
	require.EqualValues(t, 1, stats.CacheMiss)
	require.EqualValues(t, 1, stats.CacheHit)
	require.EqualValues(t, 1, stats.RequestsFilled)
	require.EqualValues(t, 0, stats.RequestsAbandoned)
}

func TestParallelReverseDNSLookup(t *testing.T) {
	d := NewReverseDNSCache(1*time.Second, 1*time.Second, -1)
	var answer1 []string
	var answer2 []string
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		answer1 = d.Lookup("8.8.8.8")
		wg.Done()
	}()
	go func() {
		answer2 = d.Lookup("8.8.8.8")
		wg.Done()
	}()

	wg.Wait()

	t.Log(answer1)
	t.Log(answer2)

	require.Equal(t, []string{"dns.google."}, answer1)
	require.Equal(t, []string{"dns.google."}, answer2)

	require.Len(t, d.cache, 1)

	stats := d.Stats()

	require.EqualValues(t, 1, stats.CacheMiss)
	require.EqualValues(t, 1, stats.CacheHit)
}

func TestUnavailableDNSServerRespectsTimeout(t *testing.T) {
	d := NewReverseDNSCache(0, 1, -1)

	result := d.Lookup("192.153.33.3")

	require.Equal(t, []string{}, result)
}

func TestCleanupHappens(t *testing.T) {
	ttl := 100 * time.Millisecond
	d := NewReverseDNSCache(ttl, 1*time.Second, -1)
	_ = d.Lookup("8.8.8.8")
	d.waitForWorkers()

	require.Len(t, d.cache, 1)

	time.Sleep(ttl) // wait for cache entry to expire.
	d.cleanup()
	require.Len(t, d.expireList, 0)

	stats := d.Stats()

	require.EqualValues(t, 1, stats.CacheExpire)
	require.EqualValues(t, 1, stats.CacheMiss)
	require.EqualValues(t, 0, stats.CacheHit)
}

func TestCachePassthrough(t *testing.T) {
	d := NewReverseDNSCache(0, 1*time.Second, -1)
	_ = d.Lookup("8.8.8.8")
	d.waitForWorkers()

	require.Len(t, d.cache, 0)

	stats := d.Stats()
	require.EqualValues(t, 1, stats.CacheMiss)
	require.EqualValues(t, 0, stats.CacheHit)
}

func TestLookupTimeout(t *testing.T) {
	// 1ns timeout
	d := NewReverseDNSCache(1*time.Second, 1, -1)
	d.Lookup("8.8.8.8")

	for d.Stats().RequestsAbandoned != 1 {
		time.Sleep(1 * time.Millisecond)
	}
	require.EqualValues(t, 1, d.Stats().RequestsAbandoned)
}
