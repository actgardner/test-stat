package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

// Includes all the fields of the Golang TestEvent structure
// https://github.com/golang/go/blob/2ebe77a2fda1ee9ff6fd9a3e08933ad1ebaea039/src/cmd/test2json/main.go
type TestResult struct {
	Time    time.Time // encodes as an RFC3339-format string
	Action  string
	Package string
	Test    string
	Elapsed float64 // seconds
	Output  string

	Repo   string
	Branch string
	Commit string
	Run    string
	Stage  string
}

func main() {
	projectId := os.Getenv("BIGQUERY_PROJECT")
	dataset := os.Getenv("BIGQUERY_DATASET")
	table := os.Getenv("BIGQUERY_TABLE")
	testBucket := os.Getenv("TEST_RESULTS_BUCKET")

	repo := os.Getenv("CIRCLE_REPOSITORY_URL")
	stage := os.Getenv("CIRCLE_STAGE")
	branch := os.Getenv("CIRCLE_BRANCH")
	run := os.Getenv("CIRCLE_BUILD_NUM")
	commit := os.Getenv("CIRCLE_SHA1")

	input, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("Error opening file %q: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	defer input.Close()

	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("Error creating GCS client: %v\n", err)
		os.Exit(1)
	}

	objectName := fmt.Sprintf("%v/%v/%v-%v-%v", repo, branch, run, stage, commit)
	w := gcsClient.Bucket(testBucket).Object(objectName).NewWriter(ctx)
	func() {
		defer w.Close()

		fmt.Printf("Uploading test data to GCS...\n")

		var result TestResult
		jsonReader := json.NewDecoder(input)
		jsonWriter := json.NewEncoder(w)
		for jsonReader.More() {
			if err := jsonReader.Decode(&result); err != nil {
				if err != nil {
					fmt.Printf("Error decoding JSON record: %v\n", err)
					os.Exit(1)
				}
			}

			result.Repo = repo
			result.Stage = stage
			result.Run = run
			result.Commit = commit
			result.Branch = branch

			if err := jsonWriter.Encode(result); err != nil {
				if err != nil {
					fmt.Printf("Error writing JSON record: %v\n", err)
					os.Exit(1)
				}
			}
		}
	}()

	bqClient, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		fmt.Printf("Error creating BigQuery client: %v\n", err)
		os.Exit(1)
	}
	defer bqClient.Close()

	fmt.Printf("Loading data to BigQuery...\n")

	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%v/%v", testBucket, objectName))
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := bqClient.Dataset(dataset).Table(table).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(ctx)
	if err != nil {
		fmt.Printf("BigQuery loader failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Job: %v...\n", job)

	status, err := job.Wait(ctx)
	if err != nil {
		fmt.Printf("BigQuery watch failed: %v\n", err)
		os.Exit(1)
	}

	if status.Err() != nil {
		fmt.Printf("BigQuery upload completed with error: %v\n", status.Err())
		os.Exit(1)
	}
}
