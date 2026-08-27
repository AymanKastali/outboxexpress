package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEncodeCloudEvent_FieldOrderAndPrecision(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{
		ID:         "e-1",
		Type:       "com.example.thing.happened",
		Source:     "/services/example",
		Subject:    "thing-1",
		Time:       time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC),
		SchemaName: "example/thing.happened",
		SchemaBase: "https://schemas.example.test",
		Version:    1,
	}, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}

	const want = `{"specversion":"1.0","id":"e-1","type":"com.example.thing.happened",` +
		`"source":"/services/example","subject":"thing-1","time":"2026-08-26T10:15:30.100Z",` +
		`"dataschema":"https://schemas.example.test/example/thing.happened/1.json",` +
		`"datacontenttype":"application/json","data":{"k":"v"}}`
	if string(got) != want {
		t.Errorf("mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestEncodeCloudEvent_TrailingZerosSurvive(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{
		Time: time.Date(2026, 8, 26, 10, 15, 30, 0, time.UTC),
	}, nil)
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// RFC3339Nano would render this "…:30Z". The contract is fixed-width.
	if decoded["time"] != "2026-08-26T10:15:30.000Z" {
		t.Errorf("time = %v, want fixed millisecond precision", decoded["time"])
	}
}

func TestEncodeCloudEvent_OmitsEmptyTraceparent(t *testing.T) {
	got, err := EncodeCloudEvent(Attributes{Time: time.Now()}, nil)
	if err != nil {
		t.Fatalf("EncodeCloudEvent: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := decoded["traceparent"]; ok {
		t.Error("traceparent must be omitted rather than sent empty")
	}
}
