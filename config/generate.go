package config

import (
	"fmt"
)

func Singlenode(numShards int, hostsPerShard int) []ClusterConfig {
	redisPort := 8697
	flotilla := 1103
	hosts := make([]Host, numShards*hostsPerShard)
	var hIdx = 0
	for s := 0; s < numShards; s++ {
		for h := 0; h < hostsPerShard; h++ {
			hosts[hIdx] = Host{
				RedisAddr:    fmt.Sprintf("127.0.0.1:%d", redisPort),
				FlotillaAddr: fmt.Sprintf("127.0.0.1:%d", flotilla),
				Group:        fmt.Sprintf("group%d", h),
			}
			redisPort++
			flotilla++
			hIdx++
		}
	}
	return AutoCluster(100, hosts)
}

// builds a config for each host
// hosts must contain the same number of hosts for each distinct group, we recommend 3 groups
func AutoCluster(numSlots int, hosts []Host) []ClusterConfig {
	if len(hosts) == 0 {
		return make([]ClusterConfig, 0, 0)
	}
	fmt.Printf("Building autocluster for hosts %+v\n", hosts)
	// make sure we have exactly 1 in each replica set
	countByGroup := make(map[string]int)
	for _, h := range hosts {
		prevCount, ok := countByGroup[h.Group]
		if ok {
			countByGroup[h.Group] = prevCount + 1
		} else {
			countByGroup[h.Group] = 1
		}
	}
	fmt.Printf("Count by group %+v\n", countByGroup)
	//numGroups := len(countByGroup)
	countPerGroup := -1
	for _, count := range countByGroup {
		if countPerGroup == -1 {
			countPerGroup = count
		} else {
			if countPerGroup != count {
				panic(fmt.Sprintf("require same # of peers in each group!  count by group: %+v", countByGroup))
			}
		}
	}

	// split the hosts into shards
	shards := make([]Shard, len(hosts)/countPerGroup, len(hosts)/countPerGroup)
	for i := 0; i < len(shards); i++ {
		// figure out slots, any number in range numSlots % i == 0 is ours
		mySlots := make([]uint32, 0)
		for slot := 0; slot < numSlots; slot++ {
			fmt.Printf("i: %d slot %d\n",i,slot)
			//if (i == 0 && slot == 0) || (i != 0 && slot%i == 0) {
			if slot % len(shards) == i {
				mySlots = append(mySlots, uint32(slot))
			}
		}
		// identify our hosts
		myHosts := make([]Host, countPerGroup)
		for idx, h := range hosts {
			// if same modulo as us, our shard
			if idx%len(shards) == i%len(shards) {
				myHosts = append(myHosts, h)
			}
		}
		shards[i] = Shard{mySlots, myHosts}
	}
	// make a config for each host
	ret := make([]ClusterConfig, len(hosts), len(hosts))
	for i := 0; i < len(hosts); i++ {
		ret[i] = ClusterConfig{uint32(numSlots), hosts[i], shards}
	}
	return ret
}
