package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	jwtmiddleware "github.com/auth0/go-jwt-middleware"
	"github.com/auth0/go-jwt-middleware/validator"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
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
	Url      string   `json:"url"`
}

const (
	INDEX       = "around" // 应用名字，找数据用
	TYPE        = "post"
	DISTANCE    = "200km"
	ES_URL      = "http://34.27.222.128:9200"
	BUCKET_NAME = "post-images-381306"
)

var mySigningKey = []byte("secret")

func main() {
	// create a client client handle
	// sniff 回调函数
	// 用API操作服务
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
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

	r := mux.NewRouter()

	keyFunc := func(ctx context.Context) (interface{}, error) {
		// Our token must be signed using this data.
		return mySigningKey, nil
	}
	// Set up the validator.
	jwtValidator, err := validator.New(
		keyFunc,
		validator.HS256,
		"https://<issuer-url>/",
		[]string{"<audience>"},
	)
	if err != nil {
		log.Fatalf("failed to set up the validator: %v", err)
	}

	// Set up the middleware.
	jwtMiddleware := jwtmiddleware.New(jwtValidator.ValidateToken)

	// http.HandlerFunc("/post", handlerPost)
	// jwtMiddleware验证用户提交token和key是否一致
	r.Handle("/post", jwtMiddleware.CheckJWT(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.CheckJWT(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	http.Handle("/", r)
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
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Aloow-Headers", "Content-Type, Authorization")

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	r.ParseMultipartForm(32 << 20) // max memory: 32M

	// Parse form data
	fmt.Printf("Received one post request %s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	// create unique string
	id := (uuid.New()).String()

	// read image file
	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusInternalServerError)
		fmt.Printf("Image is not available %v\n", err)
		panic(err)
	}
	defer file.Close()

	ctx := context.Background()
	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}

	p.Url = attrs.MediaLink

	// save to ES
	saveToES(p, id)
}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// create a clent
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	// create a bucket instance
	bucket := client.Bucket(bucketName)

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)
	wc := obj.NewWriter(ctx)
	if _, err = io.Copy(wc, r); err != nil {
		return nil, nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, nil, err
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
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

	fmt.Printf("Query took %v milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts \n", searchResult.TotalHits())

	// searchResult返回Post类型,reflection
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // 取post类型
		p := item.(Post) // p = (Post)item 类型转换
		fmt.Printf("Post by %s: %s at lat %v and lon %v \n",
			p.User, p.Message, p.Location.Lat, p.Location.Lon)
		// perform filtering based on key words such as web spam etc
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") //前端访问任何脚本位置
	w.Write(js)
}

func containsFilteredWords(s *string) bool {
	filteredWords := []string{
		"hehe",
		"xxxx",
	}
	for _, word := range filteredWords {
		if strings.Contains(*s, word) {
			return true
		}
	}
	return false
}
