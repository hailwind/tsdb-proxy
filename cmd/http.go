package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

type HttpService struct {
	Conf *Conf
}

type QueryMetric struct {
	Metric     string
	Aggregator string
	Tags       map[string]string
}

type OpentsQuery struct {
	Start   int64
	End     int64
	Queries []QueryMetric
}

type InfluxQuery struct {
	Metrics      []string
	Tags         map[string]string
	MetricMap    map[string]string
	TagMap       map[string]string
	Start        int64
	End          int64
	PrimaryURL   string
	SecondaryURL string
	DB           string
	Measurement  string
}

type OpentsDps struct {
	Metric        string                 `json:"metric"`
	Tags          map[string]string      `json:"tags"`
	AggregateTags []string               `json:"aggregateTags"`
	Dps           map[string]interface{} `json:"dps"`
}

func NewHttpService(conf *Conf) (hs *HttpService) {
	hs = &HttpService{
		Conf: conf,
	}
	return
}

func (hs *HttpService) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/query", hs.HandlerQuery)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	iq := hs.decodePayload(body)
	// data, _ := json.Marshal(iq)
	// fmt.Println(string(data))

	results := iq.QueryFromBackend(false)
	if len(results) == 1 && len(results[0].Series) == 1 {
		odps := iq.convertResult(&results[0].Series[0])
		data, _ := json.Marshal(odps)
		fmt.Fprintln(w, string(data))
	} else {
		log.Println("WARNING....")
	}

}

func (hs *HttpService) decodePayload(payload []byte) (iq *InfluxQuery) {
	oq := &OpentsQuery{}
	err := json.Unmarshal(payload, oq)
	if err != nil {
		log.Println(err)
	} else {
		iq = hs.convertOpents(oq)
	}
	return
}

func (hs *HttpService) convertOpents(oq *OpentsQuery) (iq *InfluxQuery) {
	var primary_url, secondary_url, db, measurement string
	var metrics []string
	mmap := make(map[string]string)
	tmap := make(map[string]string)
	tags := make(map[string]string)
	conf := hs.Conf
	for _, qm := range oq.Queries {
		im, tagmap := conf.GetInfluxMeta(qm.Metric)
		primary_url = im["primary_url"]
		secondary_url = im["secondary_url"]
		db = im["db"]
		measurement = im["measurement"]
		metrics = append(metrics, im["metric"])
		mmap[im["metric"]] = qm.Metric
		for k, v := range qm.Tags {
			if if_t, ok := tagmap[k]; ok {
				tags[if_t] = v
				tmap[if_t] = k
			} else {
				tags[k] = v
				tmap[k] = k
			}
		}
	}
	iq = &InfluxQuery{
		Start:        prepareTime(oq.Start),
		End:          prepareTime(oq.End),
		Metrics:      metrics,
		Tags:         tags,
		MetricMap:    mmap,
		TagMap:       tmap,
		PrimaryURL:   primary_url,
		SecondaryURL: secondary_url,
		DB:           db,
		Measurement:  measurement,
	}
	return
}

func (iq *InfluxQuery) convertResult(row *models.Row) *[]OpentsDps {
	var result []OpentsDps
	for idx, column := range row.Columns {
		if column == "time" {
			continue
		} else {
			tags := make(map[string]string)
			dps := make(map[string]interface{})
			for k, v := range iq.Tags {
				tags[iq.TagMap[k]] = v
			}
			for _, value := range row.Values {
				timex := fmt.Sprintf("%s", value[0])
				dps[timex] = value[idx]
			}
			odps := OpentsDps{
				Metric:        iq.MetricMap[column],
				AggregateTags: []string{},
				Tags:          tags,
				Dps:           dps,
			}
			result = append(result, odps)
		}
	}
	// data, err := json.Marshal(result)
	// fmt.Println(string(data), "\n", "error: ", err)
	return &result
}

func prepareTime(intime int64) int64 {
	sstr := strconv.FormatInt(intime, 10)
	diff := 19 - len(sstr)
	return intime * int64(math.Pow10(diff))
}

func (iq *InfluxQuery) createSQL() string {
	metrics_sql := strings.Join(iq.Metrics, ",")
	var tags []string
	for k, v := range iq.Tags {
		tags = append(tags, fmt.Sprintf("%s='%s'", k, v))
	}
	where_sql := strings.Join(tags, " and ")
	full_sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s and time>%d and time<%d",
		metrics_sql, iq.Measurement, where_sql, iq.Start, iq.End)
	//log.Println(full_sql)
	return full_sql
}

func (iq *InfluxQuery) QueryFromBackend(secondary bool) (results []client.Result) {
	if iq.PrimaryURL == "" {
		log.Println("PrimaryURL is nil")
		return
	}
	url := iq.PrimaryURL
	if secondary {
		if iq.SecondaryURL == "" {
			log.Println("SecondaryURL is nil")
			return
		}
		log.Println("Query from secondary, url: ", iq.SecondaryURL)
		url = iq.SecondaryURL
	}
	//log.Println(url)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: url,
	})

	if err != nil {
		log.Println("Error creating InfluxDB Client: ", err.Error(), " URL: ", url)
	} else {
		defer c.Close()
		sql := iq.createSQL()
		q := client.NewQuery(sql, iq.DB, "s")
		response, err := c.Query(q)
		if err == nil && response.Error() == nil {
			results = response.Results
		} else {
			log.Println("Query error, URL: ", url)
			if !secondary {
				return iq.QueryFromBackend(true)
			}
		}
	}
	return
}

func HealthCheck(backends *[]Backend) {
	for {
		ping(backends)
		time.Sleep(time.Second * 2)
	}
}

func ping(backends *[]Backend) {
	for idx, backend := range *backends {
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: backend.URL,
		})

		if err != nil {
			fmt.Println("Error creating InfluxDB Client: ", err.Error())
		}
		defer c.Close()
		_, _, err = c.Ping(0)
		if err != nil {
			backend.Alive = false
		} else {
			backend.Alive = true
		}
		(*backends)[idx] = backend
	}
}
