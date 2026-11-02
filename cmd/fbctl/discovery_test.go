package main

import (
	"strings"
	"testing"
)

func TestParseKeywordCSV(t *testing.T) {
	kws, err := parseKeywordCSV(strings.NewReader("term,language,group,enabled\nErbil,en,erbil\nهەولێر,ckb,erbil,true\nHit,en,anbar,false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(kws) != 3 || kws[0].Term != "Erbil" || kws[0].Group != "erbil" || !kws[0].Enabled ||
		kws[1].Language != "ckb" || kws[2].Enabled {
		t.Fatalf("%+v", kws)
	}
	for _, bad := range []string{"Erbil\n", "Erbil,fr\n", "term,language\nErbil,fr\n", "term,language\nErbil,en,g,maybe\n"} {
		if _, err := parseKeywordCSV(strings.NewReader(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
