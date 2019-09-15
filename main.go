package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/gin-contrib/cache"
	"github.com/gin-contrib/cache/persistence"
	"github.com/icrowley/fake"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"github.com/go-redis/redis"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
	"strings"
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

type Mlog struct{
	ID         bson.ObjectId `bson:"_id,omitempty"`
	Path       string
	Query      string
	Timestamp  time.Time
	Duration   time.Duration
	Method     string
	StatusCode int
	ClientIP   string
	UserAgent  string
	Referrer   string
	Size       int
}

var wg sync.WaitGroup
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

	//tlsConfig := &tls.Config{}

	m := os.Getenv("MONGODB_URI")

	if m == "" {
		log.Fatal("no Mongodb")
	}
	session, err := mgo.Dial(m)


	redisUrl := os.Getenv("REDISCLOUD_URL")

	if redisUrl == "" {
		log.Fatal("no redisUrl")
	}

	// cache time in hours
	hours := 12

	splitRedis := strings.Split(strings.Replace(redisUrl,"redis://rediscloud:", "",1), "@")
	redisPassword := splitRedis[0]
	redisBaseUrl := splitRedis[1]

	store := persistence.NewRedisCache(redisBaseUrl, redisPassword, time.Duration(hours)*time.Hour)

	redisOptions, err := redis.ParseURL(redisUrl)
	if redisUrl == "" {
		log.Fatal("redis client connect failure")
	}
	client := redis.NewClient(redisOptions)

	if err != nil {
		log.Fatal(err)
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	//router.Use(gin.Logger())
	router.Use(mongoLogger(session))
	router.LoadHTMLGlob("static/*html")

	router.Static("/static", "./static")

	router.GET("/", cache.CachePageWithoutHeader(store, time.Duration(hours)*time.Hour, func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl.html", nil)
	}))

	router.GET("/cache-clear", func(c *gin.Context) {
		client.FlushAll()
	})

	router.GET("/count", cache.CachePageWithoutHeader(store, time.Duration(hours)*time.Hour,  func(c *gin.Context){
		count(session, c)
	}))

	router.GET("/csv", cache.CachePageWithoutHeader(store, time.Duration(hours)*time.Hour,  func(c *gin.Context){
		mongocsv(session, c)
	}))

	router.GET("/db", func(c *gin.Context) {

		c.Redirect(307, "/")

		if err != nil {
			log.Fatal(err)
		}

		go func() {

			page := 0

			for i := 0; i < 101; i++ {
				wg.Add(1)

				page++

				uri := fmt.Sprintf("%s%d%s", "https://api.flippa.com/v3/listings?page%5Bnumber=", page, "&page%5Bsize%5D=200")
				//
				//fmt.Println(uri)

				time.Sleep(1 * time.Second)

				go crawl(uri, &wg, session)
			}
			wg.Wait()
			client.FlushAll()
			download(session)
		}()

	})

	router.GET("/flippa.csv", func(c *gin.Context) {
		c.File("./static/data/flippa.csv")

	})

	router.GET("/download", func(c *gin.Context) {

		if err != nil {
			log.Fatal(err)
		}

		go func() {
			download(session)

		}()

		c.Redirect(307, "/")
	})

	router.GET("/api", cache.CachePageWithoutHeader(store, time.Duration(hours)*time.Hour,  func(c *gin.Context) {
		router.Use(gzip.Gzip(gzip.BestCompression))
		router.Use(jsonHeader())
		rest(session, c)
	}))

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	fmt.Printf("running on port %s", port)
	// listen on server 0.0.0.0:$PORT
	err = router.Run("0.0.0.0:" + port)
	if err != nil {
		log.Fatal(err)
	}

}

func crawl(uri string, wg *sync.WaitGroup, session *mgo.Session) {
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
		log.Fatal(err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Println(err)
		}
	}()

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

		sessionCopy := session.Copy()
		c := sessionCopy.DB("heroku_5rdx8xtc").C("flippa_data")

		for _, v := range data.Data {
			update := bson.M{"id": v.ID}
			_, err := c.Upsert(update, v)

			if err != nil {
				log.Fatal(err)
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

func rest(session *mgo.Session, c *gin.Context) {

	mongoUrl := os.Getenv("MONGODB_URI")
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		log.Fatal(err)
	}
	sessionCopy := session.Copy()

	m := sessionCopy.DB("heroku_5rdx8xtc").C("flippa_data")

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

func mongocsv(session *mgo.Session, c *gin.Context) {

	out := c.Writer

	// Create Writer
	writer := csv.NewWriter(out)

	time.Local = time.UTC

	mongoUrl := os.Getenv("MONGODB_URI")
	session, err := mgo.Dial(mongoUrl)

	if err != nil {
		log.Fatal(err)
	}

	sessionCopy := session.Copy()
	m := sessionCopy.DB("heroku_5rdx8xtc").C("flippa_data")

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

func download(session *mgo.Session) {

	// After cmd flag parse
	file, err := os.Create("./static/data/flippa.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	// Create Writer
	writer := csv.NewWriter(file)

	time.Local = time.UTC

	sessionCopy := session.Copy()
	m := sessionCopy.DB("heroku_5rdx8xtc").C("flippa_data")

	var headers []string

	// Auto Detect Headerline

	var h bson.M

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

func count(session *mgo.Session, c *gin.Context) {

	mongoUrl := os.Getenv("MONGODB_URI")
	session, err := mgo.Dial(mongoUrl)

	if err != nil {
		log.Fatal(err)
	}
	
	sessionCopy := session.Copy()
	m := sessionCopy.DB("heroku_5rdx8xtc").C("flippa_data")

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

func mongoLogger(session *mgo.Session) gin.HandlerFunc {
	return func(c *gin.Context) {

		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()
		duration := time.Now().Sub(start)
		method := c.Request.Method
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		userAgent := c.Request.UserAgent()
		referrer := c.Request.Referer()
		size := c.Writer.Size()

		sessionCopy := session.Copy()

		m := sessionCopy.DB("heroku_5rdx8xtc").C("logs")
		//err =  m.Insert(logs)

		err := m.Insert(&Mlog{Path: path, Query: query, Timestamp: time.Now(), Duration: duration,
			Method: method, StatusCode: statusCode, ClientIP: clientIP,
			UserAgent: userAgent, Referrer: referrer, Size: size})

		//not serious enough to fatal
		if err != nil {
			fmt.Printf("mongo logging error")
		}
		fmt.Println(err)
	}
}