package assert

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

// Equal verifies equality of two objects.
func Equal[T any](t *testing.T, a, b T) {
	if !reflect.DeepEqual(a, b) {
		t.Helper()
		t.Fatalf("%v != %v", a, b)
	}
}

// NotEqual verifies objects are not equal.
func NotEqual[T any](t *testing.T, a T, b T) {
	if reflect.DeepEqual(a, b) {
		t.Helper()
		t.Fatalf("%v == %v", a, b)
	}
}

// ErrorContains checks whether the given error contains the specified string.
func ErrorContains(t *testing.T, err error, str string) {
	if err == nil {
		t.Helper()
		t.Fatalf("Error is nil")
	} else if !strings.Contains(err.Error(), str) {
		t.Helper()
		t.Fatalf("Error does not contain string: %s", str)
	}
}

// Panics checks whether the given function panics.
func Panics(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Helper()
			t.Fatalf("Function did not panic")
		}
	}()
	f()
}

// InDelta checks whether two float64 values are within a given delta of each other.
func InDelta(t *testing.T, expected, actual, delta float64) {
	if math.Abs(expected-actual) > delta {
		t.Helper()
		t.Fatalf("Expected %v to be within %v of %v", actual, delta, expected)
	}
}
