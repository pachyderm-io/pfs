# Creating a shuffle pipeline

This example demonstrates how shuffle pipelines i.e. a pipeline that shuffles, combines files without downloading/uploading can be created.

## Create fruits input repo
```shell
pachctl create repo fruits
pachctl put file fruits@master -f mango.jpeg
pachctl put file fruits@master -f apple.jpeg
```

## Create pricing input repo
```shell
pachctl create repo pricing
pachctl put file pricing@master -f mango.json
pachctl put file pricing@master -f apple.json
```


## Create shuffle pipeline
```shell
pachctl create pipeline -f shuffle.json
```

Let's take a closer look at that pipeline:

```json
{
  "input": {
    "union": [
      {
        "pfs": {
          "glob": "/*.jpeg",
          "repo": "fruits",
          "empty_files": true
        }
      },
      {
        "pfs": {
          "glob": "/*.json",
          "repo": "pricing",
          "empty_files": true
        }
      }
    ]
  },
  "pipeline": {
    "name": "lazy_shuffle"
  },
  "transform": {
    "image": "ubuntu:18.04",
    "cmd": ["/bin/bash"],
    "stdin": [
      "echo 'process fruits if any'",
      "fn=$(find  -L /pfs -not -path \"*/\\.*\"  -type f \\( -path '*/fruits/*' \\))",
      "for f in $fn; do fruit_name=$(basename $f .jpeg); mkdir -p /pfs/out/$fruit_name/; ln -s $f /pfs/out/$fruit_name/img.jpeg; done",
      "echo 'process pricing if any'",
      "fn=$(find  -L /pfs -not -path \"*/\\.*\"  -type f \\( -path '*/pricing/*' \\))",
      "for f in $fn; do fruit_name=$(basename $f .json); mkdir -p /pfs/out/$fruit_name/; ln -s $f /pfs/out/$fruit_name/cost.json; done"
    ]
  }
}
```

Notice that both of our inputs have the `"empty_files"` field set to `true`,
this means that we'll get files with the correct name but no content. If your
shuffle can be done looking only at the names of the files, without considering
content, specifying `"empty_files"` will massively improve its performance.

## Results

### List job
`pachctl list job` indicates no data download or upload was performed

| ID                               | OUTPUT COMMIT                            | STARTED        | DURATION  | RESTART | PROGRESS  | DL | UL | STATE   |
|----------------------------------|------------------------------------------|----------------|-----------|---------|-----------|----|----|---------|
| 60617fd06155451d8358cc714bf9b670 | shuffle/f56e97fa9e234eb6ad902640d4fba2ac | 10 seconds ago | 4 seconds | 0       | 4 + 0 / 4 | 0B | 0B | success |



### Output files:
`pachctl list file "shuffle@master:*"` will show shuffled file:

| NAME             | TYPE | SIZE     |
|------------------|------|----------|
| /mango/cost.json | file | 22B      |
| /mango/img.jpeg  | file | 7.029KiB |
| /apple/cost.json | file | 23B      |
| /apple/img.jpeg  | file | 4.978KiB |
