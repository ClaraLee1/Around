package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"time"

	"github.com/dgrijalva/jwt-go"
	"gopkg.in/olivere/elastic.v7"
)

const (
	TYPE_USER = "user"
)

var (
	usernamePattern = regexp.MustCompile(`^[a-z0-9_]+$`).MatchString
)

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Age      int    `json:"age"`
	Gender   string `json:"gender"`
}

// check whether the user is valid
func checkUser(username, password string) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		panic(err)
	}

	// search with a term query
	termQuery := elastic.NewTermQuery("username", username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		panic(err)
	}

	var tyu User
	for _, item := range queryResult.Each(reflect.TypeOf(tyu)) {
		u := item.(User)
		return u.Password == password && u.Username == username
	}

	return false
}

// add a new user, return ture if success
func addUser(user User) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false
	}

	ctx := context.Background()

	termQuery := elastic.NewTermQuery("username", user.Username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do(ctx)
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}

	if queryResult.TotalHits() > 0 {
		fmt.Printf("User %s already exists, cannot create duplicate user\n", user.Username)
		return false
	}

	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE_USER).
		Id(user.Username).
		BodyJson(user).
		Refresh("true").
		Do(ctx)
	if err != nil {
		fmt.Printf("ES save user failed %v\n", err)
		return false
	}

	return true
}

// if signup is successful, a new session is created
func signupHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received on signup request")

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}

	if u.Username != "" && u.Password != "" && usernamePattern(u.Username) {
		if addUser(u) {
			fmt.Println("User added successfully")
			w.Write([]byte("User added successfully"))
		} else {
			fmt.Println("Failed to add a new user")
			http.Error(w, "Failed to add a new user", http.StatusInternalServerError)
		}
	} else {
		fmt.Println("Empty password or username or invalid username")
		http.Error(w, "Empty password or username or invalid username", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one signup request")

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}

	if checkUser(u.Username, u.Password) {
		token := jwt.New(jwt.SigningMethodHS256)
		claims := token.Claims.(jwt.MapClaims)
		claims["username"] = u.Username
		claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

		tokenString, _ := token.SignedString(mySigningKey)
		w.Write([]byte(tokenString))
	} else {
		fmt.Println("Invalid password or username")
		http.Error(w, "Invalid password or username", http.StatusForbidden) //403
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}
