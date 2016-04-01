package require

import (
	"reflect"
	"runtime"
	"testing"
)

func Equal(tb testing.TB, expected interface{}, actual interface{}, msgAndArgs ...interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		fatal(
			tb,
			msgAndArgs,
			"Not equal: %#v (expected)\n"+
				"        != %#v (actual)", expected, actual)
	}
}

func EqualOneOf(tb testing.TB, expecteds []interface{}, actual interface{}, msgAndArgs ...interface{}) {
	equal := false
	for _, expected := range expecteds {
		if reflect.DeepEqual(expected, actual) {
			equal = true
			break
		}
	}
	if !equal {
		fatal(
			tb,
			msgAndArgs,
			"Not equal 1 of: %#v (expecteds)\n"+
				"        != %#v (actual)", expecteds, actual)
	}
}

func NoError(tb testing.TB, err error, msgAndArgs ...interface{}) {
	if err != nil {
		fatal(tb, msgAndArgs, "No error is expected but got %v", err)
	}
}

func YesError(tb testing.TB, err error, msgAndArgs ...interface{}) {
	if err == nil {
		fatal(tb, msgAndArgs, "Error is expected but got %v", err)
	}
}

func NotNil(tb testing.TB, object interface{}, msgAndArgs ...interface{}) {
	success := true

	if object == nil {
		success = false
	} else {
		value := reflect.ValueOf(object)
		kind := value.Kind()
		if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
			success = false
		}
	}

	if !success {
		fatal(tb, msgAndArgs, "Expected value not to be nil.")
	}
}

func Nil(tb testing.TB, object interface{}, msgAndArgs ...interface{}) {
	if object == nil {
		return
	}
	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return
	}

	fatal(tb, msgAndArgs, "Expected value to be nil.")
}

func True(tb testing.TB, value bool, msgAndArgs ...interface{}) {
	if !value {
		fatal(tb, msgAndArgs, "Should be true.")
	}
}

func False(tb testing.TB, value bool, msgAndArgs ...interface{}) {
	if value {
		fatal(tb, msgAndArgs, "Should be false.")
	}
}

func logMessage(tb testing.TB, msgAndArgs []interface{}) {
	if len(msgAndArgs) == 1 {
		tb.Logf(msgAndArgs[0].(string))
	}
	if len(msgAndArgs) > 1 {
		tb.Logf(msgAndArgs[0].(string), msgAndArgs[1:]...)
	}
}

func fatal(tb testing.TB, userMsgAndArgs []interface{}, msgFmt string, msgArgs ...interface{}) {
	logMessage(tb, userMsgAndArgs)
	_, file, line, ok := runtime.Caller(2)
	if ok {
		tb.Logf("%s:%d", file, line)
	}
	tb.Fatalf(msgFmt, msgArgs...)
}

