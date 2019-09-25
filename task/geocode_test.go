package task

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/savaki/dag"
	"github.com/tj/assert"
)

const smartyStreetsResponse = `[
  {
    "input_index": 0,
    "candidate_index": 0,
    "delivery_line_1": "2121 Peralta St",
    "last_line": "Oakland CA 94607-1611",
    "delivery_point_barcode": "946071611997",
    "components": {
      "primary_number": "2121",
      "street_name": "Peralta",
      "street_suffix": "St",
      "city_name": "Oakland",
      "default_city_name": "Oakland",
      "state_abbreviation": "CA",
      "zipcode": "94607",
      "plus4_code": "1611",
      "delivery_point": "99",
      "delivery_point_check_digit": "7"
    },
    "metadata": {
      "record_type": "H",
      "zip_type": "Standard",
      "county_fips": "06001",
      "county_name": "Alameda",
      "carrier_route": "C003",
      "congressional_district": "13",
      "building_default_indicator": "Y",
      "rdi": "Commercial",
      "elot_sequence": "0076",
      "elot_sort": "A",
      "latitude": 37.81634,
      "longitude": -122.29019,
      "precision": "Zip9",
      "time_zone": "Pacific",
      "utc_offset": -8,
      "dst": true
    },
    "analysis": {
      "dpv_match_code": "D",
      "dpv_footnotes": "AAN1",
      "dpv_cmra": "N",
      "dpv_vacant": "N",
      "active": "N",
      "footnotes": "H#L#"
    }
  }
]`

func TestGeocoder(t *testing.T) {
	var (
		ctx       = context.Background()
		city      = "the-city"
		state     = "the-state"
		street    = "the-street"
		gotValues url.Values
	)

	transport := transportFunc(func(req *http.Request) (*http.Response, error) {
		gotValues = req.URL.Query()
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(recorder, smartyStreetsResponse)
		return recorder.Result(), nil
	})

	geocoder := SmartyStreets("blah", "blah", transport)

	t.Run("default behavior", func(t *testing.T) {
		record := &dag.Record{}
		record.Set("city", city)
		record.Set("state", state)
		record.Set("street", street)
		task := Geocode(geocoder, "street", "city", "state")
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, city, gotValues.Get("city"))
		assert.Equal(t, state, gotValues.Get("state"))
		assert.Equal(t, street, gotValues.Get("street"))

		want := map[string]interface{}{
			"city":      "Oakland",
			"county":    "Alameda",
			"latitude":  37.81634,
			"longitude": -122.29019,
			"state":     "CA",
			"street":    "2121 Peralta St",
			"zip":       "94607",
		}
		assert.Equal(t, want, record.Copy())
	})

	t.Run("with field limits", func(t *testing.T) {
		record := &dag.Record{}
		record.Set("city", city)
		record.Set("state", state)
		record.Set("street", street)
		task := Geocode(geocoder, "street", "city", "state",
			WithFields("latitude", "longitude"),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)

		want := map[string]interface{}{
			"city":      city,
			"latitude":  37.81634,
			"longitude": -122.29019,
			"state":     state,
			"street":    street,
		}
		assert.Equal(t, want, record.Copy())
	})

	t.Run("with field and transforms", func(t *testing.T) {
		record := &dag.Record{}
		record.Set("city", city)
		record.Set("state", state)
		record.Set("street", street)
		task := Geocode(geocoder, "street", "city", "state",
			WithFields("latitude", "longitude"),
			WithFieldMapper(func(field string) (string, error) {
				return field[0:3], nil
			}),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)

		want := map[string]interface{}{
			"city":   city,
			"lat":    37.81634,
			"lon":    -122.29019,
			"state":  state,
			"street": street,
		}
		assert.Equal(t, want, record.Copy())
	})
}

func TestSmartyStreets(t *testing.T) {
	var (
		authID    = "authID"
		authToken = "authToken"
		city      = "city"
		state     = "state"
		street    = "street"
		gotValues url.Values
	)

	transport := transportFunc(func(req *http.Request) (*http.Response, error) {
		gotValues = req.URL.Query()
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(recorder, smartyStreetsResponse)
		return recorder.Result(), nil
	})

	geocoder := SmartyStreets(authID, authToken, transport)
	got, err := geocoder.Lookup(context.Background(), street, city, state)
	assert.Nil(t, err)

	want := map[string]interface{}{
		"city":      "Oakland",
		"county":    "Alameda",
		"latitude":  37.81634,
		"longitude": -122.29019,
		"state":     "CA",
		"street":    "2121 Peralta St",
		"zip":       "94607",
	}

	assert.Equal(t, want, got)
	assert.Equal(t, authID, gotValues.Get("auth-id"))
	assert.Equal(t, authToken, gotValues.Get("auth-token"))
	assert.Equal(t, city, gotValues.Get("city"))
	assert.Equal(t, state, gotValues.Get("state"))
	assert.Equal(t, street, gotValues.Get("street"))
}
