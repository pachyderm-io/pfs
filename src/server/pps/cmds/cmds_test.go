// TODO(msteffen) Add tests for:
//
// - restart datum
// - stop job
// - delete job
//
// - inspect job
// - list job
//
// - create pipeline
// - create pipeline --push-images (re-enable existing test)
// - update pipeline
// - delete pipeline
//
// - inspect pipeline
// - list pipeline
//
// - start pipeline
// - stop pipeline
//
// - list datum
// - inspect datum
// - logs

package cmds

import (
	"testing"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
)

const badJSON1 = `
{
"356weryt

}
`

const badJSON2 = `{
{
    "a": 1,
    "b": [23,4,4,64,56,36,7456,7],
    "c": {"e,f,g,h,j,j},
    "d": 3452.36456,
}
`

func TestSyntaxErrorsReportedCreatePipeline(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		echo -n '{{.badJSON1}}' \
		  | ( pachctl create pipeline -f - 2>&1 || true ) \
		  | match "malformed pipeline spec"

		echo -n '{{.badJSON2}}' \
		  | ( pachctl create pipeline -f - 2>&1 || true ) \
		  | match "malformed pipeline spec"
		`,
		"badJSON1", badJSON1,
		"badJSON2", badJSON2,
	).Run())
}

func TestSyntaxErrorsReportedUpdatePipeline(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		echo -n '{{.badJSON1}}' \
		  | ( pachctl update pipeline -f - 2>&1 || true ) \
		  | match "malformed pipeline spec"

		echo -n '{{.badJSON2}}' \
		  | ( pachctl update pipeline -f - 2>&1 || true ) \
		  | match "malformed pipeline spec"
		`,
		"badJSON1", badJSON1,
		"badJSON2", badJSON2,
	).Run())
}

func TestRawFullPipelineInfo(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl garbage-collect
	`).Run())
	require.NoError(t, tu.BashCmd(`
		pachctl create repo data
		pachctl put file data@master:/file <<<"This is a test"
		pachctl create pipeline <<EOF
			{
			  "pipeline": {"name": "{{.pipeline}}"},
			  "input": {
			    "pfs": {
			      "glob": "/*",
			      "repo": "data"
			    }
			  },
			  "transform": {
			    "cmd": ["bash"],
			    "stdin": ["cp /pfs/data/file /pfs/out"]
			  }
			}
		EOF
		`,
		"pipeline", tu.UniqueString("p-")).Run())
	require.NoError(t, tu.BashCmd(`
		pachctl flush commit data@master

		# make sure the results have the full pipeline info, including version
		pachctl list job --raw --history=all \
			| match "pipeline_version"
		`).Run())
}

// TestJSONMultiplePipelines tests that pipeline specs with multiple pipelines
// in them continue to be accepted by 'pachctl create pipeline'. We may want to
// stop supporting this behavior eventually, but Pachyderm has supported it
// historically, so we should continue to support it until we formally deprecate
// it.
func TestJSONMultiplePipelines(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		pachctl create pipeline -f - <<EOF
		{
		  "pipeline": {
		    "name": "first"
		  },
		  "input": {
		    "pfs": {
		      "glob": "/*",
		      "repo": "input"
		    }
		  },
		  "transform": {
		    "cmd": [ "/bin/bash" ],
		    "stdin": [
		      "cp /pfs/input/* /pfs/out"
		    ]
		  }
		}
		{
		  "pipeline": {
		    "name": "second"
		  },
		  "input": {
		    "pfs": {
		      "glob": "/*",
		      "repo": "first"
		    }
		  },
		  "transform": {
		    "cmd": [ "/bin/bash" ],
		    "stdin": [
		      "cp /pfs/first/* /pfs/out"
		    ]
		  }
		}
		EOF

		pachctl start commit input@master
		echo foo | pachctl put file input@master:/foo
		echo bar | pachctl put file input@master:/bar
		echo baz | pachctl put file input@master:/baz
		pachctl finish commit input@master
		pachctl flush commit input@master
		pachctl get file second@master:/foo | match foo
		pachctl get file second@master:/bar | match bar
		pachctl get file second@master:/baz | match baz
		`,
	).Run())
	require.NoError(t, tu.BashCmd(`pachctl list pipeline`).Run())
}

// TestJSONStringifiedNumberstests that JSON pipelines may use strings to
// specify numeric values such as a pipeline's parallelism (a feature of gogo's
// JSON parser).
func TestJSONStringifiedNumbers(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		pachctl create pipeline -f - <<EOF
		{
		  "pipeline": {
		    "name": "first"
		  },
		  "input": {
		    "pfs": {
		      "glob": "/*",
		      "repo": "input"
		    }
		  },
			"parallelism_spec": {
				"constant": "1"
			},
		  "transform": {
		    "cmd": [ "/bin/bash" ],
		    "stdin": [
		      "cp /pfs/input/* /pfs/out"
		    ]
		  }
		}
		EOF

		pachctl start commit input@master
		echo foo | pachctl put file input@master:/foo
		echo bar | pachctl put file input@master:/bar
		echo baz | pachctl put file input@master:/baz
		pachctl finish commit input@master
		pachctl inspect pipeline first
		sleep 30
		pachctl inspect pipeline first
		echo "flush commit"
		pachctl flush commit input@master
		pachctl get file first@master:/foo | match foo
		pachctl get file first@master:/bar | match bar
		pachctl get file first@master:/baz | match baz
		`,
	).Run())
	require.NoError(t, tu.BashCmd(`pachctl list pipeline`).Run())
}

// TestJSONMultiplePipelinesError tests that when creating multiple pipelines
// (which only the encoding/json parser can parse) you get an error indicating
// the problem in the JSON, rather than an error complaining about multiple
// documents.
func TestJSONMultiplePipelinesError(t *testing.T) {
	// pipeline spec has no quotes around "name" in first pipeline
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		( pachctl create pipeline -f - 2>&1 <<EOF || true
		{
		  "pipeline": {
		    name: "first"
		  },
		  "input": {
		    "pfs": {
		      "glob": "/*",
		      "repo": "input"
		    }
		  },
		  "transform": {
		    "cmd": [ "/bin/bash" ],
		    "stdin": [
		      "cp /pfs/input/* /pfs/out"
		    ]
		  }
		}
		{
		  "pipeline": {
		    "name": "second"
		  },
		  "input": {
		    "pfs": {
		      "glob": "/*",
		      "repo": "first"
		    }
		  },
		  "transform": {
		    "cmd": [ "/bin/bash" ],
		    "stdin": [
		      "cp /pfs/first/* /pfs/out"
		    ]
		  }
		}
		EOF
		) | match "invalid character 'n' looking for beginning of object key string"
		`,
	).Run())
}

// TestYAMLPipelineSpec tests creating a pipeline with a YAML pipeline spec
func TestYAMLPipelineSpec(t *testing.T) {
	// Note that BashCmd dedents all lines below including the YAML (which
	// wouldn't parse otherwise)
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		pachctl create pipeline -f - <<EOF
		pipeline:
		  name: first
		input:
		  pfs:
		    glob: /*
		    repo: input
		transform:
		  cmd: [ /bin/bash ]
		  stdin:
		    - "cp /pfs/input/* /pfs/out"
		---
		pipeline:
		  name: second
		input:
		  pfs:
		    glob: /*
		    repo: first
		transform:
		  cmd: [ /bin/bash ]
		  stdin:
		    - "cp /pfs/first/* /pfs/out"
		EOF
		pachctl start commit input@master
		echo foo | pachctl put file input@master:/foo
		echo bar | pachctl put file input@master:/bar
		echo baz | pachctl put file input@master:/baz
		pachctl finish commit input@master
		pachctl flush commit input@master
		pachctl get file second@master:/foo | match foo
		pachctl get file second@master:/bar | match bar
		pachctl get file second@master:/baz | match baz
		`,
	).Run())
}

// TestYAMLError tests that when creating pipelines using a YAML spec with an
// error, you get an error indicating the problem in the YAML, rather than an
// error complaining about multiple documents.
//
// Note that with the new parsing method added to support free-form fields like
// TFJob, this YAML is parsed, serialized and then re-parsed, so the error will
// refer to "json" (the format used for the canonicalized pipeline), but the
// issue referenced by the error (use of a string instead of an array for 'cmd')
// is the main problem below
func TestYAMLError(t *testing.T) {
	// "cmd" should be a list, instead of a string
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		( pachctl create pipeline -f - 2>&1 <<EOF || true
		pipeline:
		  name: first
		input:
		  pfs:
		    glob: /*
		    repo: input
		transform:
		  cmd: /bin/bash # should be list, instead of string
		  stdin:
		    - "cp /pfs/input/* /pfs/out"
		EOF
		) | match "cannot unmarshal string into Go value of type \[\]json.RawMessage"
		`,
	).Run())
}

func TestTFJobBasic(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
		pachctl create repo input
		( pachctl create pipeline -f - 2>&1 <<EOF || true
		pipeline:
		  name: first
		input:
		  pfs:
		    glob: /*
		    repo: input
		tf_job:
		  apiVersion: kubeflow.org/v1
		  kind: TFJob
		  metadata:
		    generateName: tfjob
		    namespace: kubeflow
		  spec:
		    tfReplicaSpecs:
		      PS:
		        replicas: 1
		        restartPolicy: OnFailure
		        template:
		          spec:
		            containers:
		            - name: tensorflow
		              image: gcr.io/your-project/your-image
		              command:
		                - python
		                - -m
		                - trainer.task
		                - --batch_size=32
		                - --training_steps=1000
		      Worker:
		        replicas: 3
		        restartPolicy: OnFailure
		        template:
		          spec:
		            containers:
		            - name: tensorflow
		              image: gcr.io/your-project/your-image
		              command:
		                - python
		                - -m
		                - trainer.task
		                - --batch_size=32
		                - --training_steps=1000
		EOF
		) | match "not supported yet"
		`,
	).Run())
}

// TestYAMLSecret tests creating a YAML pipeline with a secret (i.e. the fix for
// https://github.com/pachyderm/pachyderm/issues/4119)
func TestYAMLSecret(t *testing.T) {
	// Note that BashCmd dedents all lines below including the YAML (which
	// wouldn't parse otherwise)
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all

		# kubectl get secrets >&2
		kubectl delete secrets/test-yaml-secret || true
		kubectl create secret generic test-yaml-secret --from-literal=my-key=my-value

		pachctl create repo input
		pachctl put file input@master:/foo <<<"foo"
		pachctl create pipeline -f - <<EOF
		  pipeline:
		    name: pipeline
		  input:
		    pfs:
		      glob: /*
		      repo: input
		  transform:
		    cmd: [ /bin/bash ]
		    stdin:
		      - "env | grep MY_SECRET >/pfs/out/vars"
		    secrets:
		      - name: test-yaml-secret
		        env_var: MY_SECRET
		        key: my-key
		EOF
		pachctl flush commit input@master
		pachctl get file pipeline@master:/vars | match MY_SECRET=my-value
		`,
	).Run())
}

// TestYAMLTimestamp tests creating a YAML pipeline with a timestamp (i.e. the
// fix for https://github.com/pachyderm/pachyderm/issues/4209)
func TestYAMLTimestamp(t *testing.T) {
	// Note that BashCmd dedents all lines below including the YAML (which
	// wouldn't parse otherwise)
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all

		# If the pipeline comes up without error, then the YAML parsed
		pachctl create pipeline -f - <<EOF
		  pipeline:
		    name: pipeline
		  input:
		    cron:
		      name: in
		      start: "2019-10-10T22:30:05Z"
		      spec: "@yearly"
		  transform:
		    cmd: [ /bin/bash ]
		    stdin:
		      - "cp /pfs/in/* /pfs/out"
		EOF
		pachctl list pipeline | match 'pipeline'
		`,
	).Run())
}

func TestEditPipeline(t *testing.T) {
	require.NoError(t, tu.BashCmd(`
		yes | pachctl delete all
	`).Run())
	require.NoError(t, tu.BashCmd(`
		pachctl create repo data
		pachctl create pipeline <<EOF
		  pipeline:
		    name: my-pipeline
		  input:
		    pfs:
		      glob: /*
		      repo: data
		  transform:
		    cmd: [ /bin/bash ]
		    stdin:
		      - "cp /pfs/data/* /pfs/out"
		EOF
		`).Run())
	require.NoError(t, tu.BashCmd(`
		EDITOR="cat -u" pachctl edit pipeline my-pipeline -o yaml \
		| match 'name: my-pipeline' \
		| match 'repo: data' \
		| match 'cmd:' \
		| match 'cp /pfs/data/\* /pfs/out'
		`).Run())
}

// func TestPushImages(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping integration tests in short mode")
// 	}
// 	ioutil.WriteFile("test-push-images.json", []byte(`{
//   "pipeline": {
//     "name": "test_push_images"
//   },
//   "transform": {
//     "cmd": [ "true" ],
// 	"image": "test-job-shim"
//   }
// }`), 0644)
// 	os.Args = []string{"pachctl", "create", "pipeline", "--push-images", "-f", "test-push-images.json"}
// 	require.NoError(t, rootCmd().Execute())
// }
