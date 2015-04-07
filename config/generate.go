package config

func Singlenode(numShards int, hostsPerShard int) []ClusterConfig {
	redisPort := 8697
	flotilla := 1103
	hosts := make([]Host, numShards*hostsPerShard)
	for s := 0; s < numShards; s++ {
		for h := 0; h < hostsPerShard; h++ {
			hosts[s*h+h] = Host{
				RedisAddr:    fmt.Sprintf("127.0.0.1:%d", redisPort),
				FlotillaAddr: fmt.Sprintf("127.0.0.1:%d", flotilla),
				Group:        fmt.Sprintf("group%d", h),
			}
		}
	}
	return AutoCluster(100, hosts)
}

// builds a config for each host
// hosts must contain the same number of hosts for each distinct group, we recommend 3 groups
func AutoCluster(int numSlots, hosts []Host) []ClusterConfig {
	if len(hosts) == 0 {
		return make([]ClusterConfig, 0, 0)
	}
	// make sure we have exactly 1 in each replica set
	countByGroup := make(map[string]uint)
	for _, h := range hosts {
		prevCount, ok := countByGroup[h.Group]
		if ok {
			countByGroup[h.Group] = prevCount + 1
		} else {
			countByGroup[h.Group] = 1
		}
	}
	numGroups := len(countByGroup)
	countPerGroup := -1
	for g, count := range countByGroup {
		if countPerGroup == -1 {
			countPerGroup = count
		} else {
			if countPerGroup != count {
				panic(fmt.Sprintf("require same # of peers in each group!  count by group: %+v", countByGroup))
			}
		}
	}

	// split the hosts into shards
	shards := make([]Shard, totalNumHosts/countPerGroup, totalNumHosts/countPerGroup)
	for i := 0; i < len(shards); i++ {
		// figure out slots, any number in range numSlots % i == 0 is ours
		mySlots := make([]uint32)
		for slot := 0; slot < numSlots; slot++ {
			if slot%i == 0 {
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
		ret[i] = ClusterConfig{numSlots, host, shards}
	}
	return ret
}
