// Copyright 2022, Matthew Winter
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package p

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"unsafe"

	"golang.org/x/oauth2/google"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// -------------------------------------------------------------------------------------------------
// Consumes a Pub/Sub message, triggering a DAG and passing the event payload.
func TriggerCloudComposerDAG(ctx context.Context, m PubSubMessage) error {

	// Retrieve Required Environment Variables
	AIRFLOW_URI := os.Getenv("AIRFLOW_URI")
	DAG_ID := os.Getenv("DAG_ID")
	VERBOSE := strings.ToUpper(strings.TrimSpace(os.Getenv("VERBOSE"))) == "TRUE"
	TARGET_URL := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns", AIRFLOW_URI, DAG_ID)

	// Output Verbose Debug Information to the Log if required
	if VERBOSE {
		log.Println(fmt.Sprintf("Environment Variables: %+v", os.Environ()))
		printContextInternals(ctx, false)
		log.Println(fmt.Sprintf("PubSubMessage: %+v", m.Data))
		log.Println(fmt.Sprintf("Target URL: %s", TARGET_URL))
		printDNSLookup(AIRFLOW_URI)
	}

	// Get a HTTP Client with the default authentication credentials.
	client, err := google.DefaultClient(ctx)
	if err != nil {
		return fmt.Errorf("google.DefaultClient: %v", err)
	}

	jsonBody, _ := json.Marshal(map[string]PubSubMessage{"conf": m})
	postBody := bytes.NewBuffer(jsonBody)

	// POST a request to trigger the execution of a dag using the stable Airflow 2 REST API.
	resp, err := client.Post(TARGET_URL, "application/json", postBody)
	if err != nil {
		return fmt.Errorf("client.Post: %v", err)
	}
	defer resp.Body.Close()

	// Read the Response Body and output to the Log
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("io.ReadAll: %v", err)
	}
	log.Println(fmt.Sprintf("Response: %v", string(body)))

	return nil
}

// -------------------------------------------------------------------------------------------------
// Output to the log the internal contents of the Context
func printContextInternals(ctx interface{}, inner bool) {
	contextValues := reflect.ValueOf(ctx).Elem()
	contextKeys := reflect.TypeOf(ctx).Elem()

	if !inner {
		log.Println(fmt.Sprintf("********** Fields for %s.%s **********", contextKeys.PkgPath(), contextKeys.Name()))
	}

	if contextKeys.Kind() == reflect.Struct {
		for i := 0; i < contextValues.NumField(); i++ {
			reflectValue := contextValues.Field(i)
			reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()

			reflectField := contextKeys.Field(i)

			if reflectField.Name == "Context" {
				printContextInternals(reflectValue.Interface(), true)
			} else {
				log.Println(fmt.Sprintf("  Field Name: %+v", reflectField.Name))
				log.Println(fmt.Sprintf("  Value: %+v", reflectValue.Interface()))
			}
		}
	} else {
		log.Println(fmt.Sprintf("Context is Empty (int)"))
	}
}

// -------------------------------------------------------------------------------------------------
// Perform a DNS lookup and output the IP's
func printDNSLookup(inputURL string) {
	url, err := url.Parse(inputURL)
	if err != nil {
		log.Println(fmt.Sprintf("Could not parse URL: %v\n", err))
		return
	}

	hostname := url.Hostname()
	ips, err := net.LookupIP(hostname)
	if err != nil {
		log.Println(fmt.Sprintf("Could not get IPs: %v\n", err))
		return
	}
	for _, ip := range ips {
		log.Println(fmt.Sprintf("%s IN A %s\n", hostname, ip.String()))
	}
}
