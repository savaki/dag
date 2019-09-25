package task

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/savaki/dag"
	"golang.org/x/xerrors"
)

type Geocoder interface {
	Lookup(ctx context.Context, street, city, state string) (map[string]interface{}, error)
}

type geocoderFunc func(ctx context.Context, street, city, state string) (map[string]interface{}, error)

func (fn geocoderFunc) Lookup(ctx context.Context, street, city, state string) (map[string]interface{}, error) {
	return fn(ctx, street, city, state)
}

func Geocode(geocoder Geocoder, street, city, state string, opts ...Option) dag.TaskFunc {
	options := makeOptions(opts...)

	return func(ctx context.Context, record *dag.Record) error {
		theStreet, _ := record.String(street)
		theCity, _ := record.String(city)
		theState, _ := record.String(state)

		if theStreet == "" || theState == "" {
			return nil
		}

		results, err := geocoder.Lookup(ctx, theStreet, theCity, theState)
		if err != nil {
			return err
		}

		for field, value := range results {
			if len(options.fields) > 0 && !containsString(options.fields, field) {
				continue
			}

			mapped, err := options.mapField(field)
			if err != nil {
				return err
			}
			record.Set(mapped, value)
		}

		return nil
	}
}

// SmartyStreets provides a SmartyStreets Geocoder.  If a nil transport is provided,
// http.DefaultTransport will be used
func SmartyStreets(authID, authToken string, transport http.RoundTripper) Geocoder {
	type Response struct {
		DeliveryLine1 string `json:"delivery_line_1"`
		Components    struct {
			PrimaryNumber     string `json:"primary_number"`
			StreetName        string `json:"street_name"`
			StreetSuffix      string `json:"street_suffix"`
			CityName          string `json:"city_name"`
			StateAbbreviation string `json:"state_abbreviation"`
			ZipCode           string `json:"zipcode"`
			Plus4Code         string `json:"plus4_code"`
		} `json:"components"`
		Metadata struct {
			ZipType               string  `json:"zip_type"`
			CountyFips            string  `json:"county_fips"`
			CountyName            string  `json:"county_name"`
			CongressionalDistrict string  `json:"congressional_district"`
			RDI                   string  `json:"rds"`
			Latitude              float64 `json:"latitude"`
			Longitude             float64 `json:"longitude"`
			Precision             string  `json:"precision"`
			TimeZone              string  `json:"time_zone"`
			UTCOffset             int     `json:"utc_offset"`
			DST                   bool    `json:"dst"`
		} `json:"metadata"`
		Analysis struct {
			DpvMatchCode string `json:"dpv_match_code"`
			DpvFootnotes string `json:"dpv_footnotes"`
			DpvCMRA      string `json:"dpv_cmra"`
			DpvVacant    string `json:"dpv_vacant"`
			Active       string `json:"active"`
			Footnotes    string `json:"footnotes"`
		} `json:"analysis"`
	}

	if transport == nil {
		transport = http.DefaultTransport
	}

	return geocoderFunc(func(ctx context.Context, street, city, state string) (map[string]interface{}, error) {
		if street == "" || state == "" {
			return nil, nil
		}

		values := url.Values{}
		values.Set("auth-id", authID)
		values.Set("auth-token", authToken)
		values.Set("candidates", "1")
		values.Set("state", state)
		values.Set("street", street)
		if city != "" {
			values.Set("city", city)
		}

		uri := "https://us-street.api.smartystreets.com/street-address?" + values.Encode()
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		if err != nil {
			return nil, xerrors.Errorf("unable to create smarty streets request: %w", err)
		}
		req = req.WithContext(ctx)

		resp, err := transport.RoundTrip(req)
		if err != nil {
			return nil, xerrors.Errorf("smarty streets api call failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, xerrors.Errorf("smarty streets returned an invalid status code, %v", resp.StatusCode)
		}

		var responses []Response
		if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
			return nil, xerrors.Errorf("unable to decode smarty streets response: %w", err)
		}

		if len(responses) == 0 {
			return nil, xerrors.Errorf("")
		}

		response := responses[0]
		return map[string]interface{}{
			"city":      response.Components.CityName,
			"county":    response.Metadata.CountyName,
			"state":     response.Components.StateAbbreviation,
			"street":    response.DeliveryLine1,
			"zip":       response.Components.ZipCode,
			"latitude":  response.Metadata.Latitude,
			"longitude": response.Metadata.Longitude,
		}, nil
	})
}
