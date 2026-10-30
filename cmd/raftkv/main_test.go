package main

import "testing"

func TestParseCluster(t *testing.T) {
	got, err := parseCluster("1=127.0.0.1:7001, 2=127.0.0.1:7002,3=localhost:7003")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1] != "127.0.0.1:7001" || got[3] != "localhost:7003" {
		t.Fatalf("got %v", got)
	}
	for _, bad := range []string{"", "1", "x=host:1", "0=host:1", "1=a:1,1=b:2", "1="} {
		if _, err := parseCluster(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
