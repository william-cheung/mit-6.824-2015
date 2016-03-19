package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"

// information about the servers of one replica group.
type tGroup struct {
	gid     int64
	servers []*ShardKV
	ports   []string
}

// information about all the servers of a k/v cluster.
type tCluster struct {
	t           *testing.T
	masters     []*shardmaster.ShardMaster
	mck         *shardmaster.Clerk
	masterports []string
	groups      []*tGroup
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

//
// start a k/v replica server thread.
//
func (tc *tCluster) start1(gi int, si int, unreliable bool) {
	s := StartServer(tc.groups[gi].gid, tc.masterports, tc.groups[gi].ports, si)
	tc.groups[gi].servers[si] = s
	s.Setunreliable(unreliable)
}

func (tc *tCluster) cleanup() {
	for gi := 0; gi < len(tc.groups); gi++ {
		g := tc.groups[gi]
		for si := 0; si < len(g.servers); si++ {
			if g.servers[si] != nil {
				g.servers[si].kill()
			}
		}
	}

	for i := 0; i < len(tc.masters); i++ {
		if tc.masters[i] != nil {
			tc.masters[i].Kill()
		}
	}
}

func (tc *tCluster) shardclerk() *shardmaster.Clerk {
	return shardmaster.MakeClerk(tc.masterports)
}

func (tc *tCluster) clerk() *Clerk {
	return MakeClerk(tc.masterports)
}

func (tc *tCluster) join(gi int) {
	tc.mck.Join(tc.groups[gi].gid, tc.groups[gi].ports)
}

func (tc *tCluster) leave(gi int) {
	tc.mck.Leave(tc.groups[gi].gid)
}

func setup(t *testing.T, tag string, unreliable bool) *tCluster {
	runtime.GOMAXPROCS(4)

	const nmasters = 3
	const ngroups = 3   // replica groups
	const nreplicas = 3 // servers per group

	tc := &tCluster{}
	tc.t = t
	tc.masters = make([]*shardmaster.ShardMaster, nmasters)
	tc.masterports = make([]string, nmasters)

	for i := 0; i < nmasters; i++ {
		tc.masterports[i] = port(tag+"m", i)
	}
	for i := 0; i < nmasters; i++ {
		tc.masters[i] = shardmaster.StartServer(tc.masterports, i)
	}
	tc.mck = tc.shardclerk()

	tc.groups = make([]*tGroup, ngroups)

	for i := 0; i < ngroups; i++ {
		tc.groups[i] = &tGroup{}
		tc.groups[i].gid = int64(i + 100)
		tc.groups[i].servers = make([]*ShardKV, nreplicas)
		tc.groups[i].ports = make([]string, nreplicas)
		for j := 0; j < nreplicas; j++ {
			tc.groups[i].ports[j] = port(tag+"s", (i*nreplicas)+j)
		}
		for j := 0; j < nreplicas; j++ {
			tc.start1(i, j, unreliable)
		}
	}

	// return smh, gids, ha, sa, clean
	return tc
}




func doConcurrent(t *testing.T, unreliable bool) {
	tc := setup(t, "concurrent-"+strconv.FormatBool(unreliable), unreliable)
	defer tc.cleanup()

	for i := 0; i < len(tc.groups); i++ {
		tc.join(i)
	}

	const npara = 11
	var ca [npara]chan bool
	for i := 0; i < npara; i++ {
		ca[i] = make(chan bool)
		go func(me int) {
			ok := true
			defer func() { ca[me] <- ok }()
			ck := tc.clerk()
			mymck := tc.shardclerk()
			key := strconv.Itoa(me)
			last := ""
			for iters := 0; iters < 3; iters++ {
				nv := strconv.Itoa(rand.Int())
				ck.Append(key, nv)
				last = last + nv
				v := ck.Get(key)
				if v != last {
					ok = false
					t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
				}

				gi := rand.Int() % len(tc.groups)
				gid := tc.groups[gi].gid
				mymck.Move(rand.Int()%shardmaster.NShards, gid)

				time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			}
		}(i)
	}

	for i := 0; i < npara; i++ {
		x := <-ca[i]
		if x == false {
			t.Fatalf("something is wrong")
		}
	}
}

func TestConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: Concurrent Put/Get/Move (unreliable) ...\n")
	doConcurrent(t, true)
	fmt.Printf("  ... Passed\n")
}
