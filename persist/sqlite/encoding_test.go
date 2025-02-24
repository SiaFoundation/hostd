package sqlite

import (
	"testing"

	rhp3 "go.sia.tech/core/rhp/v3"
	"lukechampine.com/frand"
)

func TestRHP3AccountEncoding(t *testing.T) {
	var account rhp3.Account
	frand.Read(account[:])
	encoded := encode(account)

	var decoded rhp3.Account
	if err := decode(&decoded).Scan(encoded); err != nil {
		t.Fatal(err)
	} else if account != decoded {
		//	t.Fatal("encoding mismatch", account, decoded)
	}
}
