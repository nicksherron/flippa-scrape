package main

import (
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/icrowley/fake"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Flippa struct {
	//Meta struct {
	//	PageNumber   int `json:"page_number"`
	//	PageSize     int `json:"page_size"`
	//	TotalResults int `json:"total_results"`
	//} `json:"meta"`
	//Links struct {
	//	Prev interface{} `json:"prev"`
	//	Next string      `json:"next"`
	//} `json:"links"`
	Data []struct {
		Type                  string        `json:"type"`
		ID                    string        `json:"id"`
		AppDownloadsPerMonth  interface{}   `json:"app_downloads_per_month"`
		AverageProfit         interface{}   `json:"average_profit"`
		AverageRevenue        interface{}   `json:"average_revenue"`
		BidCount              int           `json:"bid_count"`
		BusinessModel         string        `json:"business_model"`
		BuyItNowPrice         interface{}   `json:"buy_it_now_price"`
		Confidential          bool          `json:"confidential"`
		CurrentPrice          int           `json:"current_price"`
		EndsAt                time.Time     `json:"ends_at"`
		EstablishedAt         time.Time     `json:"established_at"`
		ExternalURL           string        `json:"external_url"`
		HasVerifiedRevenue    bool          `json:"has_verified_revenue"`
		HasVerifiedTraffic    bool          `json:"has_verified_traffic"`
		HTMLURL               string        `json:"html_url"`
		Hostname              string        `json:"hostname"`
		Industry              string        `json:"industry"`
		PageViewsPerMonth     interface{}   `json:"page_views_per_month"`
		PostAuctionNegotiable bool          `json:"post_auction_negotiable"`
		ProfitPerMonth        interface{}   `json:"profit_per_month"`
		PropertyName          string        `json:"property_name"`
		PropertyType          string        `json:"property_type"`
		ReserveMet            bool          `json:"reserve_met"`
		RevenuePerMonth       interface{}   `json:"revenue_per_month"`
		RevenueSources        []interface{} `json:"revenue_sources"`
		SaleMethod            string        `json:"sale_method"`
		StartsAt              time.Time     `json:"starts_at"`
		Status                string        `json:"status"`
		Summary               string        `json:"summary"`
		Title                 string        `json:"title"`
		UniquesPerMonth       interface{}   `json:"uniques_per_month"`
		Watching              bool          `json:"watching"`
		//Images                struct {
		//	Thumbnail struct {
		//		URL     string        `json:"url"`
		//		Targets []interface{} `json:"targets"`
		//	} `json:"thumbnail"`
		//} `json:"images"`
		//Relationships struct {
		//	Seller struct {
		//		Data struct {
		//			Type string `json:"type"`
		//			ID   string `json:"id"`
		//		} `json:"data"`
		//		Links struct {
		//			Self string `json:"self"`
		//		} `json:"links"`
		//	} `json:"seller"`
		//	TagsSiteType struct {
		//		Links struct {
		//			Self string `json:"self"`
		//		} `json:"links"`
		//	} `json:"tags_site_type"`
		//	CategoriesTopLevel struct {
		//		Links struct {
		//			Self string `json:"self"`
		//		} `json:"links"`
		//	} `json:"categories_top_level"`
		//	Upgrades struct {
		//		Links struct {
		//			Self string `json:"self"`
		//		} `json:"links"`
		//	} `json:"upgrades"`
		//	TagsMonetization struct {
		//		Links struct {
		//			Self string `json:"self"`
		//		} `json:"links"`
		//	} `json:"tags_monetization"`
		//} `json:"relationships"`
		//Links struct {
		//	Self string `json:"self"`
		//} `json:"links"`
	} `json:"data"`
}


var wg sync.WaitGroup
var dbsend = flag.Bool("db", true, "send to mongodb")
var flattened = make(map[string]interface{})

var aggregate = []bson.M{{"$project": bson.M{"_id": 0.0, "Name": "$propertyname",
	"Title":   "$title",
	"ID":      "$id",
	"Website": "$externalurl",
	"Established at": bson.M{
		"$dateToString": bson.M{
			"format": "%m-%d-%Y",
			"date":   "$establishedat"}},
	"Profit Per Month":        "$profitpermonth",
	"Revenue Per Month":       "$revenuepermonth",
	"Page Views Per Month":    "$pageviewspermonth",
	"Uniques Per Month":       "$uniquespermonth",
	"App Downloads per month": "$appdownloadspermonth",
	"Verified Revenue":        "$hasverifiedrevenue",
	"Verified Traffic":        "$hasverifiedtraffic",
	"Business Model":          "$businessmodel",
	"Type":                    "$propertytype",
	"Industry":                "$industry",
	"Summary":                 "$summary",
	"URL":                     "$htmlurl",
	"Sale Method":             "$salemethod",
	"Status":                  "$status",
	"Current Price":           "$currentprice",
	"Buy it Now":              "$buyitnowprice",
	"Bid Count":               "$bidcount",
	"Starts At": bson.M{
		"$dateToString": bson.M{
			"format": "%m-%d-%Y",
			"date":   "$startsat"}},
	"Ends At": bson.M{
		"$dateToString": bson.M{
			"format": "%m-%d-%Y",
			"date":   "$endsat"}}}}}

func main() {

	flag.Parse()

	tlsConfig := &tls.Config{}

	mongoUrl, err := mgo.ParseURL(os.Getenv("MONGODB_URL"))

	if err != nil {
		log.Fatal(err)
	}

	mongoUrl.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(mongoUrl)



	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("static/*html")

	router.Static("/static", "./static")


	router.GET("/nocache", rest)

	router.GET("/count", count)

	router.GET("/csv", mongocsv)

	router.GET("/db", func(context *gin.Context) {

		context.Redirect(307, "/")

		if err != nil {
			log.Fatal(err)
		}
		db := *dbsend

		go func() {

			page := 0

			for i := 0; i < 101; i++ {
				wg.Add(1)

				page++

				uri := fmt.Sprintf("%s%d%s", "https://api.flippa.com/v3/listings?page%5Bnumber=", page, "&page%5Bsize%5D=200")
				//
				//fmt.Println(uri)

				time.Sleep(1 * time.Second)

				go crawl(uri, &wg, session, db)
			}
			wg.Wait()
			download(context)
		}()

	})

	router.GET("/flippa.csv", func(c *gin.Context) {
		c.File("./static/data/flippa.csv")

	})

	router.GET("/download", func(context *gin.Context) {

		if err != nil {
			log.Fatal(err)
		}

		go func() {
			download(context)

		}()

		context.Redirect(307, "/")
	})

	router.Use(gzip.Gzip(gzip.BestCompression))
	router.Use(jsonHeader())
	router.GET("/api", rest)

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	// listen on server 0.0.0.0:$PORT
	router.Run("0.0.0.0:" + port)

}

func crawl(uri string, wg *sync.WaitGroup, session *mgo.Session, db bool) {
	defer wg.Done()

	client := http.Client{}

	req, err := http.NewRequest("GET", uri, nil)

	if err != nil {
		fmt.Println("could'nt make req")
		log.Fatal(err)
	}

	req.Header.Set("User-Agent", fake.UserAgent())
	req.Header.Set("X-Forwarded-For", fake.IPv4())

	resp, err := client.Do(req)

	if err != nil {
		log.Println(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 {

		j, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			log.Fatal(err)
		}

		var data Flippa

		err = data.UnmarshalJSON(j)
		//err = json.Unmarshal(j, &data)

		if err != nil {
			log.Fatal(err)
		}

		//update := bson.M{"id": v.ID}

		//var format Doc

		sessionCopy := session.Copy()
		c := sessionCopy.DB("data").C("flippa_data")

		for _, v := range data.Data {
			update := bson.M{"id": v.ID}
			_, err := c.Upsert(update, v)

			if err != nil {
				//fmt.Println("fucking insert error")
				fmt.Println(err)
			}

		}

	}
}

func jsonHeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", " application/json; charset=utf-8")
		c.Next()

	}
}

func rest(c *gin.Context) {

	tlsConfig := &tls.Config{}

	mongoUrl, err := mgo.ParseURL(os.Getenv("MONGODB_URL"))

	if err != nil {
		log.Fatal(err)
	}

	mongoUrl.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(mongoUrl)

	sessionCopy := session.Copy()

	m := sessionCopy.DB("data").C("flippa_data")

	var data []bson.M
	pipe := m.Pipe(aggregate)

	err = pipe.All(&data)

	b, err := json.MarshalIndent(data, "", "  ")

	if err != nil {
		log.Println("error unmarshalling")
	}

	_, err = c.Writer.Write(b)
	if err != nil {
		log.Println("error writing")
	}

}

func flatten(input bson.M, lkey string, flattened *map[string]interface{}) {
	for rkey, value := range input {
		key := lkey + rkey
		if _, ok := value.(string); ok {
			(*flattened)[key] = value.(string)
		} else if _, ok := value.(float64); ok {
			(*flattened)[key] = value.(float64)
		} else if _, ok := value.(int); ok {
			(*flattened)[key] = value.(int)
		} else if _, ok := value.(int64); ok {
			(*flattened)[key] = value.(int64)
		} else if _, ok := value.(bool); ok {
			(*flattened)[key] = value.(bool)
		} else if _, ok := value.(time.Time); ok {
			(*flattened)[key] = value.(time.Time).Format("2006-01-02T15:04:05Z07:00")
		} else if _, ok := value.(bson.ObjectId); ok {
			(*flattened)[key] = value.(bson.ObjectId).Hex()
		} else if _, ok := value.([]interface{}); ok {
			for i := 0; i < len(value.([]interface{})); i++ {
				if _, ok := value.([]string); ok {
					stringI := string(i)
					(*flattened)[stringI] = value.(string)
				} else if _, ok := value.([]int); ok {
					stringI := string(i)
					(*flattened)[stringI] = value.(int)
				} else {
					if _, ok := value.([]interface{})[i].(bson.M); ok {
						flatten(value.([]interface{})[i].(bson.M), key+"."+strconv.Itoa(i)+".", flattened)
					}
				}
			}
		} else {
			if value != nil {
				flatten(value.(bson.M), key+".", flattened)
			} else {
				(*flattened)[key] = ""
			}
		}
	}
}

func mongocsv(c *gin.Context) {

	//// After cmd flag parse
	//file, err := os.Create("flippa.csv")
	//
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer file.Close()
	//
	//// Create Writer
	//writer := csv.NewWriter(file)

	out := c.Writer

	// Create Writer
	writer := csv.NewWriter(out)

	time.Local = time.UTC

	tlsConfig := &tls.Config{}

	mongoUrl, err := mgo.ParseURL(os.Getenv("MONGODB_URL"))

	if err != nil {
		log.Fatal(err)
	}

	mongoUrl.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(mongoUrl)

	sessionCopy := session.Copy()
	m := sessionCopy.DB("data").C("flippa_data")

	var headers []string

	// Auto Detect Headerline
	var h bson.M
	err = m.Find(nil).Select(bson.M{
		"_id": 0,
	}).One(&h)
	if err != nil {
		log.Fatal(err)
	}

	flatten(h, "", &flattened)
	for key := range flattened {
		headers = append(headers, key)
	}

	// Default sort the headers
	// Otherwise accessing the headers will be
	// different each time.
	sort.Strings(headers)
	// write headers to file
	err = writer.Write(headers)
	if err != nil {
		log.Fatal(err)
	}

	writer.Flush()
	// log.Print(headers)

	var limit int
	if s, err := strconv.Atoi(c.Query("limit")); err == nil {
		switch {
		case s >= 1:
			limit = s
		default:
			limit = 0
		}
	} else {
		limit = 1000
	}

	var skip int
	if s, err := strconv.Atoi(c.Query("skip")); err == nil {
		switch {
		case s >= 1:
			skip = s
		default:
			skip = 0
		}
	} else {
		skip = 0
	}

	var docs []bson.M

	// Create a cursor using Find query
	//cursor := m.Find(nil).Skip(skip).Limit(limit).Select(bson.M{
	//	"_id": 0,
	//
	//}).All(&docs)

	err = m.Find(nil).Skip(skip).Limit(limit).Select(bson.M{
		"_id": 0,
	}).All(&docs)

	if err != nil {
		log.Fatal(err)
	}

	// Iterate over all items in a collection
	//var i bson.M
	count := 0

	for _, i := range docs {

		//for cursor.Next(&m) {
		var record []string

		flatten(i, "", &flattened)
		for _, header := range headers {
			record = append(record, fmt.Sprint(flattened[header]))
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatal(err)
		}

		writer.Flush()

		count++

	}

	fmt.Printf("%d record(s) exported\n", count)

}

func download(c *gin.Context) {

	// After cmd flag parse
	file, err := os.Create("./static/data/flippa.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create Writer
	writer := csv.NewWriter(file)

	time.Local = time.UTC

	tlsConfig := &tls.Config{}

	dialInfo := &mgo.DialInfo{
		Addrs: []string{"upwork-shard-00-00-n65o2.mongodb.net:27017",
			"upwork-shard-00-01-n65o2.mongodb.net:27017",
			"upwork-shard-00-02-n65o2.mongodb.net:27017"},
		Database: "admin",
		Username: "nick",
		Password: "tjNbspWK4LIVHSd9",
	}
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(dialInfo)

	sessionCopy := session.Copy()
	m := sessionCopy.DB("data").C("flippa_data")

	var headers []string

	// Auto Detect Headerline

	var h bson.M
	//err = m.Find(nil).Select(bson.M{
	//	"_id": 0,
	//
	//}).One(&h)
	//if err != nil {
	//	log.Fatal(err)
	//}

	pipe := m.Pipe(aggregate)
	err = pipe.One(&h)
	if err != nil {
		log.Fatal(err)
	}

	flatten(h, "", &flattened)
	for key := range flattened {
		headers = append(headers, key)
	}

	// Default sort the headers
	// Otherwise accessing the headers will be
	// different each time.
	sort.Strings(headers)
	// write headers to file
	err = writer.Write(headers)
	if err != nil {
		log.Fatal(err)
	}

	writer.Flush()

	var docs []bson.M

	pipe = m.Pipe(aggregate)
	err = pipe.All(&docs)

	if err != nil {
		log.Fatal(err)
	}

	// Iterate over all items in a collection
	//var i bson.M
	count := 0

	for _, i := range docs {

		//for cursor.Next(&m) {
		var record []string

		flatten(i, "", &flattened)
		for _, header := range headers {
			record = append(record, fmt.Sprint(flattened[header]))
		}
		err = writer.Write(record)
		if err != nil {
			log.Fatal(err)
		}

		writer.Flush()

		count++

	}

	fmt.Printf("%d record(s) exported\n", count)

}

func count(c *gin.Context) {

	tlsConfig := &tls.Config{}

	mongoUrl, err := mgo.ParseURL(os.Getenv("MONGODB_URL"))

	if err != nil {
		log.Fatal(err)
	}

	mongoUrl.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(mongoUrl)

	sessionCopy := session.Copy()
	m := sessionCopy.DB("data").C("flippa_data")

	n, err := m.Count()

	if err != nil {
		log.Fatal(err)
	}

	t := strconv.Itoa(n) + "  records available for download.\n1000   records available for preview."

	_, err = c.Writer.WriteString(t)
	if err != nil {
		log.Fatal(err)
	}

}
