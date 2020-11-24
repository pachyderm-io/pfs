# Create a Join Pipeline

In this example, we will create a join pipeline.
A join pipeline executes your code on files that match
a specific naming pattern. For example, you have two
repositories — one that stores readings from temperature
sensors and the other that stores parameters for these
sensors. You want to match these two datasets and process
them together. For simplicity, the files have some
simple dummy data. For each file in the `readings` repository that,
the pipeline code takes the sum of all lines and multiplies
them by the sum of the lines in the matching file in the
`parameters` repository. 

The repositories have the following structures:

* `readings`:

    ```shell
    └── ID1234
        ├── file1.txt
        ├── file2.txt
        ├── file3.txt
        ├── file4.txt
        ├── file5.txt
        └── file6.txt
    ```

 * `parameters`:

    ```shell
    ├── file1.txt
    ├── file2.txt
    ├── file3.txt
    ├── file4.txt
    ├── file5.txt
    └── file6.txt
    ```

## Prerequisites

You must have the following components deployed on your computer
to complete this example:

* Minikube or Docker Desktop
* Pachyderm 1.9.5 or later

## Step 1 - Prepare your Environment

`Makefile` in this directory creates dummy files for you
to test this examples. The `Makefile` targets create all the
needed files, build and execute a Docker container
that runs the code in [joins.py]().

To set up your environment, complete the following steps:

1. Verify that you have all the components described in [Prerequisites](#prerequisites).
1. If you have not done so already, clone the Pachyderm repository:

   ```shell
   git clone https://github.com/pachyderm/pachyderm.git
   ```

   Or, if you prefer to use SSH, run:

   ```shell
   git clone git@github.com:pachyderm/pachyderm.git
   ```

1. Go to the `pachyderm/examples/joins` directory.
1. Create the dummy data by running:

   ```shell
   make setup
   ```

   This command reates the `readings/ID1234` and  `parameters` directories
   with the structures described above.

1. Go to [Step 2](#step-2-set-up-the-pachyderm-repositories).

## Step 2 - Set up the Pachyderm Repositories

You need to create the `readings` and `parameters` repositories
in Pachyderm and upload the dummy data that you have generated
in the previous step into those repositories.

To set up the Pachyderm repositories, complete the following steps:

1. Create the repositories:

   ```shell
   $ pachctl create repo readings
   $ pachctl create repo parameters
   ```

1. Upload the test files to the `parameters` repository:

    ```shell
    $ pachctl put file -r parameters@master:/ -f parameters
    ```

 1. Verify that the files were uploaded:

    ```shell
    $ pachctl list file parameters@master
    NAME       TYPE SIZE
    /file1.txt file 9B
    /file2.txt file 9B
    /file3.txt file 9B
    /file4.txt file 9B
    /file5.txt file 9B
    /file6.txt file 9B
    ```

1. From the `readings` directory, run the following command to upload
   the test files to the `readings` repository:

   ```shell
   $ pachctl put file -r readings@master -f ID1234
   ```

1. Verify that the files were uploaded:

   ```shell
   $ pachctl list file readings@master:/ID1234
   NAME              TYPE SIZE
   /ID1234/file1.txt file 9B
   /ID1234/file2.txt file 9B
   /ID1234/file3.txt file 9B
   /ID1234/file4.txt file 9B
   /ID1234/file5.txt file 9B
   /ID1234/file6.txt file 9B
   ```

## Step 3 - Run the Pipeline

When your repositories are ready, create and run the pipeline
from the [joins.json](joins.json) file by completing the following steps:

1. From the `examples/joins/` directory, run:

   ```shell
   $ pachctl create pipeline -f joins.json
   ```
   The pipeline will be in state "running" when it's ready,
   which you can check with the following command.

   ```shell
   $ pachctl list pipeline
   NAME  VERSION INPUT                                       CREATED       STATE / LAST JOB   DESCRIPTION                                                                       
   joins 1       (parameters:/(*).txt ⋈ readings:/*/(*).txt) 4 seconds ago running / starting A pipeline that combines files from two repositories that match a naming pattern. 
   ```

   In the example above, the joins pipeline runs in the `pipeline-joins-v1-xx264`
   pod. When the `STATUS` of the pipeline changes to `Running`, it indicate that
   the pipeline is working correctly.

1. After Pachyderm pulls the correct container for your pipeline, it starts a job
   for the newly created pipeline and processes the data. You can view the job
   status by running the following command:

   ```shell
   $ pachctl list job
   ID                               PIPELINE STARTED       DURATION           RESTART PROGRESS  DL   UL  STATE
   7390b0c29ac247c893422a5c04565719 joins    2 seconds ago Less than a second 0       6 + 0 / 6 108B 24B success
   ```

   In the output above, you can see that Pachyderm processes six datums.
   Pachyderm creates one datum for each matching file in each repository.

1. Get the contents of the file to view the result:

   ```shell
   $ pachctl list file joins@master
   NAME       TYPE SIZE
   /file1.txt file 4B
   /file2.txt file 4B
   /file3.txt file 4B
   /file4.txt file 4B
   /file5.txt file 4B
   /file6.txt file 4B
   ```

   The sample pipeline code we are running creates one output file for each
   datum.

1. Get the contents of the file to view the result:

   ```
   $ pachctl get file joins@master:/file1.txt
   225
   ```
