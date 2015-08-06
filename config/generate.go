package config

import (
	"fmt"
	"os"
)

func Singlenode(numShards int, hostsPerShard int, dataRoot string) []ClusterConfig {
	redisPort := 8679
	flotilla := 1103
	hosts := make([]Host, numShards*hostsPerShard)
	dataDirs := make([]string, numShards*hostsPerShard)
	var hIdx = 0
	for s := 0; s < numShards; s++ {
		for h := 0; h < hostsPerShard; h++ {
			hosts[hIdx] = Host{
				RedisAddr:    fmt.Sprintf("127.0.0.1:%d", redisPort),
				FlotillaAddr: fmt.Sprintf("127.0.0.1:%d", flotilla),
				Group:        fmt.Sprintf("group%d", h),
			}
			dataDirs[hIdx] = fmt.Sprintf("%s/%d", dataRoot, redisPort)
			os.MkdirAll(dataDirs[hIdx], 0777)
			redisPort++
			flotilla++
			hIdx++
		}
	}
	return AutoCluster(100, hosts, dataDirs)
}

// builds a config for each host
// hosts must contain the same number of hosts for each distinct group, i.e., len(hosts) % numDistinctGroups == 0 must hold true.
// We recommend 3 groups and len(hosts) % 3 == 0
func AutoCluster(numSlots int, hosts []Host, dataDirs []string) []ClusterConfig {
	// split the hosts into shards
	shards := Shards(numSlots, hosts)
	// make a config for each host
	ret := make([]ClusterConfig, len(hosts), len(hosts))
	for i := 0; i < len(hosts); i++ {
		ret[i] = ClusterConfig{
			NumSlots: uint32(numSlots),
			Me:       hosts[i],
			Datadir:  dataDirs[i],
			Shards:   shards,
		}
	}
	return ret
}

func Shards(numSlots int, hosts []Host) []Shard {
	if len(hosts) == 0 {
		return make([]Shard, 0, 0)
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
			if slot%len(shards) == i {
				mySlots = append(mySlots, uint32(slot))
			}
		}
		// identify our hosts
		myHosts := make([]Host, 0, 0)
		shardStartIdx := i * countPerGroup
		for h := shardStartIdx; h < shardStartIdx+countPerGroup; h++ {
			myHosts = append(myHosts, hosts[h])
		}
		shards[i] = Shard{mySlots, myHosts}
	}
	return shards
}
