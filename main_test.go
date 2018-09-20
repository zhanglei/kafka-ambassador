package main

import (
	"io/ioutil"
	"net/http"
)

func sendRequest(client *http.Client, addr string) {
	res, err := client.Get(addr)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		panic("request failed")
	}

	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	err = res.Body.Close()
	if err != nil {
		panic(err)
	}
}
