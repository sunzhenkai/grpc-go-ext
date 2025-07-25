package wxns

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

var DefaultNodeWeight = 100

func fetchWeight(addr string) int {
	url := "http://" + addr + "/rpc/meta"
	client := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("weightloader: error fetching weight from %s: %v", url, err)
		return DefaultNodeWeight
	}
	defer resp.Body.Close()

	var res struct {
		Weight int `json:"weight"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Printf("weightloader: error decoding weight from %s: %v", url, err)
		return DefaultNodeWeight
	}
	return res.Weight
}
