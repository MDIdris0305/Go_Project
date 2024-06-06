package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/olekukonko/tablewriter"
)

type data_fetched struct {
	TripID                   string        `json:"trip_id"`
	TaxiID                   string        `json:"taxi_id"`
	TripStartTimestamp       CustomTime    `json:"trip_start_timestamp"`
	TripEndTimestamp         CustomTime    `json:"trip_end_timestamp"`
	TripSeconds              CustomInt     `json:"trip_seconds"`
	TripMiles                CustomFloat64 `json:"trip_miles"`
	PickupCensusTract        string        `json:"pickup_census_tract"`
	DropoffCensusTract       string        `json:"dropoff_census_tract"`
	PickupCommunityArea      CustomInt     `json:"pickup_community_area"`
	DropoffCommunityArea     CustomInt     `json:"dropoff_community_area"`
	Fare                     CustomFloat64 `json:"fare"`
	Tips                     CustomFloat64 `json:"tips"`
	Tolls                    CustomFloat64 `json:"tolls"`
	Extras                   CustomFloat64 `json:"extras"`
	TripTotal                CustomFloat64 `json:"trip_total"`
	PaymentType              string        `json:"payment_type"`
	Company                  string        `json:"company"`
	PickupCentroidLatitude   CustomFloat64 `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude  CustomFloat64 `json:"pickup_centroid_longitude"`
	PickupCentroidLocation   Location      `json:"pickup_centroid_location"`
	DropoffCentroidLatitude  CustomFloat64 `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude CustomFloat64 `json:"dropoff_centroid_longitude"`
	DropoffCentroidLocation  Location      `json:"dropoff_centroid_location"`
}

type CustomTime struct {
	time.Time
}

type Location struct {
	Type        string     `json:"type"`
	Coordinates [2]float64 `json:"coordinates"`
}

const ctLayout = "2006-01-02T15:04:05.000"

// UnmarshalJSON parses the time string into a CustomTime struct
func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	str := string(b[1 : len(b)-1])
	t, err := time.Parse(ctLayout, str)
	if err != nil {
		return err
	}
	ct.Time = t
	return nil
}

type CustomInt struct {
	Int int
}

// UnmarshalJSON parses the int string into a CustomInt struct
func (ci *CustomInt) UnmarshalJSON(b []byte) error {
	str := string(b[1 : len(b)-1])
	i, err := strconv.Atoi(str)
	if err != nil {
		return err
	}
	ci.Int = i
	return nil
}

// CustomFloat64 is a wrapper to handle JSON numbers that might be strings
type CustomFloat64 struct {
	Float64 float64
}

// UnmarshalJSON parses the float string into a CustomFloat64 struct
func (cf *CustomFloat64) UnmarshalJSON(b []byte) error {
	str := string(b[1 : len(b)-1])
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return err
	}
	cf.Float64 = f
	return nil
}

func main() {
	var (
		Hostname = "localhost"
		Port     = 5432
		Username = "mdidris"
		Password = "postgres"
		Database = "extraction"
	)
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database)
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Set up timer
	timer := time.NewTimer(10 * time.Minute)

	go func() {
		<-timer.C
		log.Println("Timer expired. Exiting program.")
		cancel()
	}()

	createTable(ctx, db)
	fetchAndPrinttaxitrips(ctx, db)
}

func createTable(ctx context.Context, db *sql.DB) {
	_, err := db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS taxi_trips (
            trip_id TEXT PRIMARY KEY,
            taxi_id TEXT,
            trip_start_timestamp TIMESTAMP,
            trip_end_timestamp TIMESTAMP,
            trip_seconds INTEGER,
            trip_miles FLOAT,
            pickup_census_tract TEXT,
            dropoff_census_tract TEXT,
            pickup_community_area INTEGER,
            dropoff_community_area INTEGER,
            fare FLOAT,
            tips FLOAT,
            tolls FLOAT,
            extras FLOAT,
            trip_total FLOAT,
            payment_type TEXT,
            company TEXT,
            pickup_centroid_latitude FLOAT,
            pickup_centroid_longitude FLOAT,
            pickup_centroid_location FLOAT,
            dropoff_centroid_latitude FLOAT,
            dropoff_centroid_longitude FLOAT,
            dropoff_centroid_location FLOAT
        );
    `)
	if err != nil {
		log.Fatal(err)
	}
}

func fetchAndPrinttaxitrips(ctx context.Context, db *sql.DB) {
	offset := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled. Exiting fetchAndPrinttaxitrips.")
			return
		default:
			url := fmt.Sprintf("https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=100&$offset=%d", offset)
			log.Printf("Fetching data from: %s\n", url)
			resp, err := http.Get(url)
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()
			log.Println("Response received from the API")

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}

			var trips []data_fetched
			err = json.Unmarshal(body, &trips)
			if err != nil {
				log.Fatal(err)
			}

			if len(trips) == 0 {
				break // Exit the loop if no more data is returned
			}

			printTable(trips)
			offset += 100
		}
	}
}

func printTable(trips []data_fetched) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Trip ID", "Taxi ID", "Start Time", "End Time", "Seconds", "Miles", "Fare", "Tips", "Total"})
	for _, trip := range trips {
		table.Append([]string{
			trip.TripID,
			trip.TaxiID,
			trip.TripStartTimestamp.Time.Format(time.RFC3339),
			trip.TripEndTimestamp.Time.Format(time.RFC3339),
			strconv.Itoa(trip.TripSeconds.Int),
			strconv.FormatFloat(trip.TripMiles.Float64, 'f', 2, 64),
			strconv.FormatFloat(trip.Fare.Float64, 'f', 2, 64),
			strconv.FormatFloat(trip.Tips.Float64, 'f', 2, 64),
			strconv.FormatFloat(trip.TripTotal.Float64, 'f', 2, 64),
		})
	}
	table.Render()
}
