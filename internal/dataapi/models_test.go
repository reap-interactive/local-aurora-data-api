package dataapi

import (
	"encoding/json"
	"testing"
)

func TestField_MarshalLongValue(t *testing.T) {
	f := LongField(42)
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `{"longValue":42}`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestField_MarshalStringValue(t *testing.T) {
	f := StringField("hello")
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `{"stringValue":"hello"}`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestField_MarshalBoolValue(t *testing.T) {
	f := BoolField(true)
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `{"booleanValue":true}`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestField_MarshalDoubleValue(t *testing.T) {
	f := DoubleField(3.14)
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["doubleValue"]; !ok {
		t.Errorf("expected doubleValue key in %s", b)
	}
}

func TestField_MarshalIsNull(t *testing.T) {
	f := NullField()
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `{"isNull":true}`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestField_MarshalBlobValue(t *testing.T) {
	f := BlobField([]byte("hello"))
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	// base64("hello") == "aGVsbG8="
	want := `{"blobValue":"aGVsbG8="}`
	got := string(b)
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestField_UnmarshalLongValue(t *testing.T) {
	var f Field
	if err := json.Unmarshal([]byte(`{"longValue":99}`), &f); err != nil {
		t.Fatal(err)
	}
	if f.LongValue == nil || *f.LongValue != 99 {
		t.Errorf("LongValue: got %v, want 99", f.LongValue)
	}
}

func TestField_UnmarshalIsNull(t *testing.T) {
	var f Field
	if err := json.Unmarshal([]byte(`{"isNull":true}`), &f); err != nil {
		t.Fatal(err)
	}
	if f.IsNull == nil || !*f.IsNull {
		t.Error("IsNull should be true")
	}
}

func TestField_UnmarshalBlobValue(t *testing.T) {
	var f Field
	if err := json.Unmarshal([]byte(`{"blobValue":"aGVsbG8="}`), &f); err != nil {
		t.Fatal(err)
	}
	if string(f.BlobValue) != "hello" {
		t.Errorf("BlobValue: got %q, want %q", f.BlobValue, "hello")
	}
}

func TestField_Value_Null(t *testing.T) {
	f := NullField()
	v, err := f.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestField_Value_Long(t *testing.T) {
	f := LongField(7)
	v, err := f.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v != int64(7) {
		t.Errorf("got %v, want 7", v)
	}
}

func TestField_Value_Array(t *testing.T) {
	f := Field{ArrayValue: &ArrayValue{LongValues: []int64{1, 2}}}
	_, err := f.Value()
	if err == nil {
		t.Error("expected error for array value, got nil")
	}
}

func TestField_Value_EmptyField(t *testing.T) {
	// A zero-value Field (no union member set) should return nil, nil.
	var f Field
	v, err := f.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("expected nil for empty field, got %v", v)
	}
}

func TestColumnMetadata_NoUnpopulatableFields(t *testing.T) {
	// Verify the trimmed ColumnMetadata struct does not have the removed fields.
	// This is a compile-time check encoded as a test: if the struct fields were
	// re-added, the code below would need updating.
	meta := ColumnMetadata{
		IsSigned:  true,
		Label:     "id",
		Name:      "id",
		Nullable:  1,
		Precision: 10,
		Type:      4,
		TypeName:  "INT4",
	}
	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"arrayBaseColumnType", "isAutoIncrement", "isCaseSensitive", "isCurrency"} {
		if _, ok := m[removed]; ok {
			t.Errorf("ColumnMetadata should not contain field %q", removed)
		}
	}
}
