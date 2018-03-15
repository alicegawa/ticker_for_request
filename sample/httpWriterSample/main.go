package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

)


func main() {
	dbHost := "http://localhost:8086"
	dbName := "httpWriterSampleDb"
	replicationFactor :=  1
	consistency := "all"

	existingDatabases, err := listDatabases(dbHost)
	if err != nil {
		log.Fatal(err)
	}
	if len(existingDatabases) > 0 {
		log.Printf("Info: there are databases already in the data store.\n Info: the result may not be adequate")
	}
	if findString(dbName, existingDatabases) == false {
		err = createDb(dbHost, dbName, replicationFactor)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
	}

	backingOffChans := make(chan bool, 100)
	backingOffDones := make(chan struct{})

	cfg := HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("dest url: %s\n", dbHost),
		Host:           dbHost,
		Database:       dbName,
		BackingOffChan: backingOffChans,
		BackingOffDone: backingOffDones,
	}
		
	w := NewHTTPWriter(cfg, consistency) 
	sampleStr := "h2o_feet,location=coyote_creek water_level=8.120,level_description=\"between 6 and 9 feet\" 1439856000\nh2o_feet,location=santa_monica water_level=2.041,level_description=\"below 3 feet\" 1439857440"
	writeData := []byte(sampleStr)
	_, err = w.WriteLineProtocol(writeData, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("all operations have finished")
}

func findString(target string, array []string) bool {
	exist := false
	for _, v := range array {
		if v == target {
			exist = true
			break
		}
	}
	return exist
}

func createDb(daemon_url, dbname string, replicationFactor int) error {
	u, err := url.Parse(daemon_url)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbname, replicationFactor))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}
	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func listDatabases(daemonUrl string) ([]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, nestedName := range listing.Results[0].Series[0].Values {
		name := nestedName[0]
		// the _internal database is skipped:
		if name == "_internal" {
			continue
		}
		ret = append(ret, name)
	}
	return ret, nil
}
