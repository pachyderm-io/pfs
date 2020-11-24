# Resuming a Spout Pipeline

Pachyderm enables you to create a special pipeline
called *the spout* that enables you to ingest streaming
data from an external source into Pachyderm. An example
of such data could be a message queue, a transaction
log, or others.

Some of these streaming platforms can keep track of
the current record so that in case of a network failure,
the progress can be resumed from where it was left off.
For example, Apache® Kafka tracks messages that
will be sent to a Kafka consumer by recording the position of
a pointer to the last record. This pointer is called an offset.
If a Kafka consumer fails, it can then read the offset and resume
from the position of the last processed message.

When you use such a system in conjunction with Pachyderm,
this progress needs to be tracked within Pachyderm as well.
A spout has an option to specify a file or directory in
which Pachyderm can keep track of the Kafka offsets or
of similar record position trackers.
This file is called a *spout marker* or just *marker*.

A spout marker records the progress of a spout pipeline,
and in case of an error, modification, or interruption
can resume where it left off.

In this example, we will create a spout pipeline with a
marker file that will track the progress of a pipeline.
Then, we will modify the pipeline and observe
how the spout continues to update records without interruption.

## Prerequisites

Before you begin, verify that you have the following components
installed on your machine:

* Pachyderm 1.9.12 or later
* Terminal

## Pipeline Overview

In this example, we will use a simple spout pipeline that
will add dots into a spout marker file. Here is how the
marker file will look like:

```
.
..
...
....
.....
......
.......
```

The script runs every thirty seconds and appends a dot (`.`) and a new line
to the marker file creating a half pyramid pattern.

After running the pipeline for some time, we will modify the Python
script so that it adds the star (`*`) symbol instead of a dot. The
resulting file should look like this:

```

.
..
...
....
.....
......
.......*
.......**
.......***
.......****
.......*****
```

## Step 1: Build the Docker Image

Pachyderm uses Docker images that you specify in your
pipeline to create Kubernetes pods that run your code.
For this example, we will use a very simple [Dockerfile](./Dockerfile)
that pulls a basic Python image and adds your code to
the container that will run your code.

To build a Docker image, complete the following steps:

1. Clone this repository:

   ```shell
   git clone git@github.com:pachyderm/pachyderm.git
   ```

1. Change the directory to `examples/spouts/spout-marker/`.

1. Build and a tag a Docker image from the Dockerfile in this directory:

   ```shell
   docker build --tag spout-marker:v1 .
   ```

   !!! note
   **Note:** Do not forget the dot in the end!

1. Push the Docker image to an image registry.

   * If you are using `minikube`, for testing you can just
   transfer your local image to a `minikube` VM:

     ```shell
     docker save spout-marker:v1 | (\
     eval $(minikube docker-env)
     docker load
     )
     ```

1. Proceed to [Step 2](#step-2-create-the-pipeline).

## Step 2: Create the Pipeline

Because spouts do not have an input and consume data from an
outside source, you do not need to create a Pachyderm
repository for this example. For this example, we do not need
to set up any messaging system, because the Python script will
generate it for us.

However, you still need to create
the spout pipeline with a marker file.
The pipeline specification for this example is stored in
[spout-marker-pipeline.json](./spout-marker-pipeline.json).

The Python script that we will use for this example is stored in
[spout-marker-example.py](./spout-marker-example.py).

When you create a spout pipeline with a marker file, Pachyderm
creates a separate branch for the spout marker and stores the
marker file in that branch.

This example includes a test spout pipeline that demonstrate markers.
To use it, complete the following steps:

1. Create a spout pipeline using this json. 

   ```json
   {
       "pipeline": {
           "name": "spoutmarker"
       },
       "transform": {
           "cmd": [ "python3", "/spout-marker-example.py" ],
           "image": "spout-marker:v1",
           "env" : {
               "OUTPUT_CHARACTER": "."
           }
       },
       "spout": {
           "marker": "mymark",
           "overwrite": true
       }
   }
   ```

   !!! note
   **Note:** In the `spout` section, you have a key-value pair
   `"marker": "mymark"`. `mymark` is the name of your marker file.
   If you use multiple marker files, `mymark` will be a
   prefix of all marker files that might be named as `mymark01`,
   `mymark02`, and so on.

1. View the list of pipelines:

   ```shell
   pachctl list pipeline
   ```

   **System response:**

   ```shell
   NAME        VERSION INPUT CREATED        STATE / LAST JOB   DESCRIPTION
   spoutmarker 1       none  2 minutes ago  running / starting
   ```

   The pipeline also creates an output repository by the same name.

1. View the list of branches created for this pipeline:

   ```shell
   pachctl list branch spoutmarker
   ```

   **System response:**

   ```
   BRANCH HEAD
   marker fb7df194725f4d2c8786e466282a7cde
   master 77935404f3ce48f09f4fd27147948e75
   ```

   Pachyderm created a `marker` branch for the
   `spoutmarker` pipeline. According to our Python code, a dot
   should be added to the `marker` file every 10 seconds. Each of these
   transactions creates a commit in both `master` and `marker` branches
   in the `spoutmarker` output repository.

   ```shell
   pachctl list commit spoutmarker@master
   ```

   **System response:**

   ```
   REPO        BRANCH COMMIT                           FINISHED           SIZE PROGRESS DESCRIPTION
   spoutmarker master f91d27382b8a40408504865783b717e9 3 minutes ago 0B   -
   spoutmarker master 333ab0ed77a24210a5ec3d613ea0c8e4 2 minutes ago 0B   -
   ```

   ```shell
   pachctl list commit spoutmarker@marker
   ```

   **System response:**

   ```
   REPO        BRANCH COMMIT                           FINISHED           SIZE PROGRESS DESCRIPTION
   spoutmarker marker dda511ef0e5c4238bc368869574125ac 3 minutes ago      4B
   spoutmarker marker e4c5f71b40e74372bff7cf6fd9dcfb89 2 minutes ago      1B
   ```

   !!! note
   **Note:** Because the script appends to the marker file, each new commit
   is larger than the previous one.

1. View the marker file:

   ```shell
   pachctl get file spoutmarker@marker:/mymark
   ```

   **System response:**

   ```shell
   .
   ..
   ...
   ....
   .....
   ```

   Run this command a few times to see that a new dot is appended every
   10 seconds.

1. (Optional) View the output.

  ```shell
   pachctl get file spoutmarker@master:/output
   ```

   **System response:**

   ```
   ......
   ```

1. Proceed to [Step 3](#step-3-modify-the-pipeline-code).

## Step 3: Modify the Pipeline Code

Now, as our pipeline is running correctly, let's try to modify it and
see if the marker file will continue to append to the new symbol.

To modify the pipeline code, complete the following steps:

1. Edit the pipeline in place, 
   changing the value of the `OUTPUT_CHARACTER` environment variable
   from `.` to `*`

   ```shell
   pachctl edit pipeline spoutmarker
   ```

   !!! note
   **Note:** You can set the environment variable `EDITOR` to use your 
   your preferred text editor.

   The new pipeline definition will look something like this in your text editor:
   
   ```json
   {
       "pipeline": {
       "name": "spoutmarker"
   },
   "transform": {
       "image": "spout-marker:v1",
       "cmd": [
           "python3",
               "/spout-marker-example.py"
        ],
        "env": {
            "OUTPUT_CHARACTER": "*"
        }
    },
    "output_branch": "master",
    "cache_size": "64M",
    "max_queue_size": "1",
    "spout": {
        "overwrite": true,
        "marker": "mymark"
    },
    "salt": "ea04c48e993c45a781b5ba315b230674",
    "datum_tries": "3"
    }
    ```
   
   !!! note
   **Note:** You can also edit the pipeline spec in
   the original `spout-marker-pipeline.json` file and use 
   `pachctl update pipeline -f spout-marker-pipeline.json`
   to accomplish the same task.
   
   
1. Once you save this file and leave the editor, 
   you'll see the pipeline restart.
   View the list of pipelines:

   ```shell
   pachctl list pipeline
   ```

   **System response:**

   ```shell
   NAME        VERSION INPUT CREATED        STATE / LAST JOB   DESCRIPTION
   spoutmarker 2       none  10 minutes ago running / starting
   ```

   Your pipeline was updated to version `2` and is
   running your updated code. You might need to wait for some time, but
   eventually, your `marker` file will look like this:

   ```shell
   pachctl get file spoutmarker@marker:/mymark
   ```

   **System response:**

   ```
   .
   ..
   ...
   ....
   .....
   ......
   ......*
   ......**
   ```

1. (Optional) View the output.

  ```shell
   pachctl get file spoutmarker@master:/output
   ```

   **System response:**

   ```
   ......**
   ```
## Summary

This example demonstrates that spout pipelines can be configured
to use a special `marker` file or directory that can keep track of
Kafka offsets or of similar record position trackers.
