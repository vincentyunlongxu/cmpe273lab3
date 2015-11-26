package main

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
)

type Data struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

type DataCollection struct {
	Datas []Data `json:"datas"`
}

var hmap map[int]Data

func Put(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	var data Data
	key, err := strconv.Atoi(p.ByName("key"))
	if err != nil {
		panic(err)
	}
	value := p.ByName("value")

	data.Key = key
	data.Value = value
	hmap[key] = data

	res, _ := json.Marshal(data)

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(200)
	fmt.Fprintf(rw, "%s", res)
}

func Get(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	targetKey, _ := strconv.Atoi(p.ByName("key"))

	var data Data
	for key, value := range hmap {
		if key == targetKey {
			data = value
		}
	}

	res, _ := json.Marshal(data)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(200)
	fmt.Fprintf(rw, "%s", res)
}

func GetInfo(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	var datas []Data
	// datas = make([]Data, len(hmap), len(hmap))
	for key, value := range hmap {
		temp := Data{
			key,
			value.Value,
		}
		datas = append(datas, temp)
	}
	var dataCollection DataCollection
	dataCollection.Datas = datas

	res, _ := json.Marshal(dataCollection)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(200)
	fmt.Fprintf(rw, "%s", res)
}

func main() {
	hmap = make(map[int]Data)

	mux := httprouter.New()
	mux.PUT("/keys/:key/:value", Put)
	mux.GET("/keys/:key", Get)
	mux.GET("/keys", GetInfo)

	server := http.Server{
		Addr:    "0.0.0.0:3000",
		Handler: mux,
	}
	server.ListenAndServe()
}
