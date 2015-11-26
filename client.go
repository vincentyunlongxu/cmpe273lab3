package main

import (
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const DEFAULT_REPLICAS = 10

type uints []uint32

func (u uints) Len() int {
	return len(u)
}

func (u uints) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u uints) Less(i, j int) bool {
	return u[i] < u[j]
}

type NodeInfo struct {
	Id     int
	Ip     string
	Weight int
}

type Data struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

type DataCollection struct {
	Datas []Data `json:"datas"`
}

func NodeConst(id int, ip string, weight int) *NodeInfo {
	return &NodeInfo{
		Id:     id,
		Ip:     ip,
		Weight: weight,
	}
}

type Consistent struct {
	NodeInfos map[uint32]NodeInfo
	numReps   int
	Resources map[int]bool
	ring      uints
	sync.RWMutex
}

func New() *Consistent {
	c := new(Consistent)
	c.NodeInfos = make(map[uint32]NodeInfo)
	c.numReps = 10
	c.Resources = make(map[int]bool)
	c.ring = uints{}
	return c
}

func (c *Consistent) Add(nodeinfo *NodeInfo) {
	c.Lock()
	defer c.Unlock()
	c.add(nodeinfo)
}

func (c *Consistent) add(nodeinfo *NodeInfo) bool {
	count := c.numReps * nodeinfo.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, nodeinfo)
		c.NodeInfos[c.hashStr(str)] = *(nodeinfo)
	}
	c.Resources[nodeinfo.Id] = true
	c.updateSortedHashes()
	return true
}

func (c *Consistent) updateSortedHashes() {
	c.ring = uints{}
	for key := range c.NodeInfos {
		c.ring = append(c.ring, key)
	}
}

func (c *Consistent) Get(key string) NodeInfo {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)

	return c.NodeInfos[c.ring[i]]
}

func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) joinStr(i int, nodeinfo *NodeInfo) string {
	return nodeinfo.Ip + "*" + strconv.Itoa(nodeinfo.Weight) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(nodeinfo.Id)
}

func (c *Consistent) search(key uint32) int {
	i := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= key
	})
	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.ring) - 1
	}
}

func Put(data Data, server string) bool {
	leftURL := "http://localhost:"
	rightURL := "/keys/" + strconv.Itoa(data.Key) + "/" + data.Value
	urlFormat := leftURL + server + rightURL
	fmt.Println(urlFormat)
	client := &http.Client{}
	req, err := http.NewRequest("PUT", urlFormat, nil)
	res, err := client.Do(req)

	if err != nil {
		log.Fatal(err)
		fmt.Println("Wrong Put Operation", err)
		panic(err)
	}

	if res != nil {
		return true
	} else {
		return false
	}

}

func main() {
	consistentHashing := New()
	consistentHashing.Add(NodeConst(0, "http://localhost:3000", 1))
	consistentHashing.Add(NodeConst(1, "http://localhost:3001", 1))
	consistentHashing.Add(NodeConst(2, "http://localhost:3002", 1))

	imap := make(map[Data]string)
	var data []Data
	data = append(data, Data{1, "a"})
	data = append(data, Data{2, "b"})
	data = append(data, Data{3, "c"})
	data = append(data, Data{4, "d"})
	data = append(data, Data{5, "e"})
	data = append(data, Data{6, "f"})
	data = append(data, Data{7, "g"})
	data = append(data, Data{8, "h"})
	data = append(data, Data{9, "i"})
	data = append(data, Data{10, "j"})

	for i := 0; i < 10; i++ {
		k := consistentHashing.Get(data[i].Value)
		imap[data[i]] = k.Ip
	}

	for k, v := range imap {
		if strings.Contains(v, "3000") {
			Put(k, "3000")
		} else if strings.Contains(v, "3001") {
			Put(k, "3001")
		} else if strings.Contains(v, "3002") {
			Put(k, "3002")
		}
	}
}
