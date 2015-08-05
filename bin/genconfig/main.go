package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jbooth/raftis/config"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"github.com/coreos/go-etcd/etcd"
	"net"
	"encoding/json"
	"time"
)

// first 3 args are configDir,
// if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard
// if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting group,host.
func main() {
	flag.Parse()
	args := flag.Args()
	fmt.Printf("Args : %+v\n", args)
	if len(args) < 4 {
		usage(args)
		return
	}
	configDir := args[0]
	dataDir := args[1]
	mode := args[2]

	fmt.Printf("mode %s\n", mode)

	if strings.ToLower(mode) == "singlenode" {
		numShards, err := strconv.Atoi(args[3])
		if err != nil {
			panic(err)
		}
		cfgs := config.Singlenode(numShards, 3, dataDir)
		err = writeConfigs(cfgs, configDir)
		if err != nil {
			panic(err)
		}
	} else if strings.ToLower(mode) == "cluster" {
		hosts, err := readHosts(args[3])
		if err != nil {
			panic(err)
		}
		dataDirs := make([]string, len(hosts), len(hosts))
		for i, _ := range hosts {
			dataDirs[i] = dataDir
		}
		cfgs := config.AutoCluster(100, hosts, dataDirs)
		err = writeConfigs(cfgs, configDir)
		if err != nil {
			panic(err)
		}
	} else if strings.ToLower(mode) == "etcd-cluster" {
		// configDir dataDir etcd-cluster group numHosts [etcdUrl]
		group := args[3]
		numHosts, err := strconv.Atoi(args[4])
		if err != nil {
			panic(err)
		}
		etcdUrl := "http://localhost:2379"
		if len(args) > 5 {
			etcdUrl = args[5]
		}
		hosts, err := readEtcdConfig(etcdUrl, group, numHosts)
		if err != nil {
			panic(err)
		}
		dataDirs := make([]string, len(hosts), len(hosts))
		for i, _ := range hosts {
			dataDirs[i] = dataDir
		}
		cfgs := config.AutoCluster(100, hosts, dataDirs)
		err = writeConfigs(cfgs, configDir)
		if err != nil {
			panic(err)
		}
	} else {
		usage(args)
		return
	}

}

func usage(args []string) {
	log.Printf("First arg should either be 'singlenode' or 'cluster or 'etcd-cluster'.  Args provided : %+v", args)
	log.Printf(`Usage: \
		first arg is MODE, either "singlenode" or "cluster" or "etcd-cluster"\
		if singlenode, 2nd arg is output directory, 3rd arg is number of shards.  we'll generate 3 datacenters "dc1,dc2,dc3" and a node in each for each shard \
		if cluster, 2nd arg is output directory, 3rd arg is a TSV file denoting datacenter,host\
		if etcd-cluster, 2nd arg is output directory, 3rd arg is an etcd url (used to read cluster configuration).`)
}

func writeConfigs(cfgs []config.ClusterConfig, configDir string) error {
	err := os.MkdirAll(configDir, 0777)
	if err != nil {
		return err
	}
	for _, cfg := range cfgs {
		outPath := configDir + "/" + cfg.Me.RedisAddr + ".conf"
		err = config.WriteConfigFile(&cfg, outPath)
		if err != nil {
			return err
		}
	}
	m, err := json.Marshal(cfgs)
	if (err != nil){
		panic(err)
	}
	log.Println("config: %s", string(m))
	return nil
}

func readHosts(hostPath string) (hosts []config.Host, err error) {
	in, err := os.Open(hostPath)
	if err != nil {
		return nil, err
	}
	bufIn := bufio.NewReader(in)
	// file is group whitespace host
	ret := make([]config.Host, 0, 0)
	for {
		line, err := bufIn.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return nil, err
			} else {
				break
			}
		} else {

			fmt.Println("parsing line " + line)

			groupHost := strings.Fields(line)
			if len(groupHost) != 2 {
				return nil, fmt.Errorf("Expected 2 fields per line, bad line %s", line)
			}
			group := groupHost[0]
			host := groupHost[1]
			h := config.Host{
				RedisAddr:    fmt.Sprintf("%s:%d", host, 8679),
				FlotillaAddr: fmt.Sprintf("%s:%d", host, 1103),
				Group:        group,
			}
			ret = append(ret, h)
		}
	}
	return ret, nil
}

func readEtcdConfig(etcdUrl string, group string, numHosts int) (hosts []config.Host, err error) {
	etcdClient := etcd.NewClient([]string{etcdUrl})
	namespacePrefix := "/raftis/cluster/"
	ip := myIp()
	if ip == ""{
		panic("ip can't be empty, something went wrong")
	}
	log.Printf("local ip resoled to " + ip)
	amIAMaster, startIndex, err := tryBecomeBootstrapMaster(etcdClient, namespacePrefix, ip)
	log.Printf("Am i a Master? %t",amIAMaster)
	if err != nil {
		panic(err)
	}
	nodesKey := namespacePrefix + "nodes"
	configKey := namespacePrefix + "hostsConfig"
	err = registerMyself(etcdClient, nodesKey, ip, group)
	if (err != nil) {
		panic(err)
	}
	log.Printf("Registered myself!")
	if amIAMaster {
		err = waitForAllToRegister(etcdClient, nodesKey, numHosts, startIndex)
		if (err != nil) {
			panic(err)
		}
		hosts, err := buildHostsConfig(etcdClient, nodesKey)
		if (err != nil) {
			panic(err)
		}
		 err = publishHostsConfig(etcdClient, configKey, hosts)
		if (err != nil) {
			panic(err)
		}
		return hosts, nil
	} else {
		log.Printf("Waiting for Master to publish config...")
		return readHostsConfig(etcdClient, configKey)
	}
}

//tries to create /raftis/cluster/bootstrapMaster key and
// returnes amIAMaster = true if suceeds, false otherwise
func tryBecomeBootstrapMaster(etcdClient *etcd.Client,
								namespacePrefix string,
								ip string) (amIAMaster bool, currentIndex uint64, err error) {
	resp, err := etcdClient.Create(namespacePrefix + "bootstrapMaster", ip, 0)
	if err != nil {
		v, ok := err.(*etcd.EtcdError)
		if ok && v.Message == "Key already exists" {
			return false, 0, nil
		} else {
			return false, 0, err
		}
	}
	return true, resp.Node.ModifiedIndex, nil
}

// registers local ip and `group` under `nodesKey`
// used by bootstrap `master` and `followers`
func registerMyself(etcdClient *etcd.Client, nodesKey string, ip string, group string) error {
	_, err := etcdClient.Create(fmt.Sprintf("%s/%s", nodesKey, ip + "::" + group), time.Now().String(), 0)
	return err
}

//recursive function waiting for `left` hosts to register themselves under `nodesKey` key.
func waitFor(etcdClient *etcd.Client, nodesKey string, left int, index uint64) error {
	if left < 1 {
		return nil
	}
	log.Printf("watinig for %d more node(s) to join on index %d...", left, index)
	resp, err := etcdClient.Watch(nodesKey, index, true, nil, nil)
	if err != nil {
		return err
	}
	lastNext := resp.Node.ModifiedIndex + 1
	return waitFor(etcdClient, nodesKey, left - 1, lastNext)
}

//wait for `numHosts` hosts to be registered under `nodesKey`. Start waiting since `startIndex`
// used by bootstrap master
func waitForAllToRegister(etcdClient *etcd.Client, nodesKey string, numHosts int, startIndex uint64) error {
	return waitFor(etcdClient, nodesKey, numHosts, startIndex)
}

//reads `nodesKey` after all nodes in cluster are registered under it, builds and returns Hosts
// used by bootstrap `master`
func buildHostsConfig(etcdClient *etcd.Client, nodesKey string) ([]config.Host, error) {
	resp, err := etcdClient.Get(nodesKey, false, true)
	if err != nil {
		return nil, err
	}
	hosts := make([]config.Host, 0, 0)
	for _, node := range resp.Node.Nodes {
		hostGroup := strings.Split(strings.TrimPrefix(node.Key, nodesKey + "/"), "::")
		host := hostGroup[0]
		group := hostGroup[1]
		//todo remove duplication
		h := config.Host{
			RedisAddr:    fmt.Sprintf("%s:%d", host, 8679),
			FlotillaAddr: fmt.Sprintf("%s:%d", host, 1103),
			Group:        group,
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

// marshals given hosts into json and sets it as a value for config key.
// used by bootstrap `master`
func publishHostsConfig(etcdClient *etcd.Client, configKey string, hosts []config.Host) error {
	marshaled, err := json.Marshal(hosts)
	if err != nil {
		return err
	}
	_, err = etcdClient.Create(configKey, string(marshaled), 0)
	return err
}

// waits for config to be published under `configKey` and once it's published reads, unmarshals and returns Hosts.
// used by bootstrap `follower`
func readHostsConfig(etcdClient *etcd.Client, configKey string) (hosts []config.Host, err error) {
	resp, err := etcdClient.Watch(configKey, 0, false, nil, nil)
	if (err != nil) {
		return nil, err
	}
	err = json.Unmarshal([]byte(resp.Node.Value), &hosts)
	return hosts, err
}

//returns "" empty string if ip can't be obtained, which should never happen
func myIp() string {
	addrs, err := net.InterfaceAddrs()
	if (err != nil){
		panic(err)
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil  {
			return ipnet.IP.String()
		}
	}
	return ""
}
