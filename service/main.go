package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"gopkg.in/olivere/elastic.v7"
)

// 定义类型
// double , raw string
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	//首字母大写才能传出去，表明对应关系
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

const (
	INDEX    = "around" // 应用名字，找数据用
	TYPE     = "post"
	DISTANCE = "200km"
	ES_URL   = "http://34.27.222.128:9200"
)

func main() {
	// create a client client handle
	// sniff 回调函数
	// 用API操作服务
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	// use IndexExists service to check if a specified index exists
	ctx := context.Background()
	exists, err := client.IndexExists(INDEX).Do(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		// create a new index, 把location标记成geo_point
		mapping := `{
			"mappings" :{
				"post" :{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do(ctx)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// 传进来格式，snake case
// {
// "use_name" : "john",
// "message" : "Test",
// "location" : {
// 	"lat": 37,
// 	"lon": -120
// }
// }

// 用户提交的数据，r， string
// 改w的副本，带星外面也变
func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request")
	// to post go
	decoder := json.NewDecoder(r.Body)
	var p Post
	// if两个statement，初始化+做判断
	// 直接编辑p地址的值
	// request获得json string

	if err := decoder.Decode(&p); err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "Post received: %s\n", p.Message)
	// create unique string
	id := (uuid.New()).String()
	// save to ES
	saveToES(&p, id)
}

func saveToES(p *Post, id string) {
	// create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL),
		elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
	// save it to Index
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh("true").
		Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to index: %s \n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search.")

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	// range
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	//Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// 进行搜索
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	ctx := context.Background()
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts \n", searchResult.TotalHits())

	// searchResult返回Post类型,reflection
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // 取post类型
		p := item.(Post) // p = (Post)item 类型转换
		fmt.Printf("Post by %s: %s at lat %v and lon %v \n",
			p.User, p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps, p)
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") //前端访问任何脚本位置
	w.Write(js)
}
