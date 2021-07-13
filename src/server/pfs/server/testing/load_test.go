package testing

import (
	"fmt"
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/testpachd"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
)

func TestLoad(t *testing.T) {
	for i, load := range loads {
		load := load
		t.Run(fmt.Sprint("Load-", i), func(t *testing.T) {
			t.Parallel()
			env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
			resp, err := env.PachClient.RunPFSLoadTest([]byte(load), nil, 0)
			require.NoError(t, err)
			require.Equal(t, "", resp.Error, fmt.Sprint("seed: ", resp.Seed))
		})
	}
}

var loads = []string{`
count: 5
operations:
  - count: 5
    operation:
      - putFile:
          files:
            count: 5
            file:
              - source: "random"
                prob: 100
        prob: 70 
      - deleteFile:
          count: 5
          directoryProb: 20 
        prob: 30 
validator: {}
fileSources:
  - name: "random"
    random:
      directory:
        depth: 3
        run: 3
      size:
        - min: 1000
          max: 10000
          prob: 30 
        - min: 10000
          max: 100000
          prob: 30 
        - min: 1000000
          max: 10000000
          prob: 30 
        - min: 10000000
          max: 100000000
          prob: 10 
`, `
count: 5
operations:
  - count: 5
    operation:
      - putFile:
          files:
            count: 10000 
            file:
              - source: "random"
                prob: 100
        prob: 100
validator: {}
fileSources:
  - name: "random"
    random:
      size:
        - min: 100
          max: 1000
          prob: 100
`, `
count: 5
operations:
  - count: 5
    operation:
      - putFile:
          files:
            count: 1
            file:
              - source: "random"
                prob: 100
        prob: 100
validator: {}
fileSources:
  - name: "random"
    random:
      size:
        - min: 10000000
          max: 100000000
          prob: 100 
`}
