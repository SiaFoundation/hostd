package accounts

import (
	"math"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestUsageTotal(t *testing.T) {
	var u Usage
	uv := reflect.ValueOf(&u).Elem()

	var total types.Currency
	for i := 0; i < uv.NumField(); i++ {
		v := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		total = total.Add(v)
		t.Log("setting field", uv.Type().Field(i).Name, "to", v.ExactString())
		uv.Field(i).Set(reflect.ValueOf(v))
	}

	if u.Total() != total {
		t.Fatal("total mismatch")
	}
}

func TestUsageAdd(t *testing.T) {
	var ua, ub Usage
	var expected Usage
	uav := reflect.ValueOf(&ua).Elem()
	ubv := reflect.ValueOf(&ub).Elem()
	ev := reflect.ValueOf(&expected).Elem()

	for i := 0; i < uav.NumField(); i++ {
		va := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		vb := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		total := va.Add(vb)

		uav.Field(i).Set(reflect.ValueOf(va))
		ubv.Field(i).Set(reflect.ValueOf(vb))
		ev.Field(i).Set(reflect.ValueOf(total))
	}

	total := ua.Add(ub)
	tv := reflect.ValueOf(total)
	for i := 0; i < tv.NumField(); i++ {
		va := ev.Field(i).Interface().(types.Currency)
		vb := tv.Field(i).Interface().(types.Currency)
		if !va.Equals(vb) {
			t.Fatalf("field %v: expected %v, got %v", tv.Type().Field(i).Name, va, vb)
		}
	}
}
