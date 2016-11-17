# Creating Analysis Pipelines
There are three steps to writing analysis in Pachyderm. 

1. Write your code
2. Generate a Docker image with your code
3. Create/add your pipeline in Pachyderm


## Writing your analysis code

Analysis code in Pachyderm can be written using any languages or libraries you want. At the end of the day, all the dependencies and code will be built into a container so it can run anywhere. We've got demonstrative [examples on GitHub](https://github.com/pachyderm/pachyderm/tree/master/examples) using bash, Python, TensorFlow, and OpenCV and we're constantly adding more.

As we touch on briefly in the [beginner tutorial](../getting_started/beginner_tutorial), your code itself only needs to read and write data from the local file system. 

For reading, Pachyderm will automatically mount each input data repo as `/pfs/<repo_name>`. Your analysis code doesn't have to deal with distributed processing as Pachyderm will automatically shard the input data across parallel containers. For example, if you've got four containers running your Python code, `/pfs/<repo_name>` in each container will only have 1/4 of the data. You have a lot of control over how that input data is split across containers. Check out our guide on :doc: `parallelization` to see the details of that.

All writes simply need to go to `/pfs/out`. This is a special directory that is available in every container. As with reading, your code doesn't have to manage parallelization, just write data to `/pfs/out` and Pachyderm will make sure it all ends up in the correct place. 

## Building a Docker Image

Please refer to [the official Docker documentation](https://docs.docker.com/engine/tutorials/dockerimages/#/building-an-image-from-a-dockerfile).

### Distributing your Image
Unless Pachyderm is running on the same Docker host that you used to build your
image you'll need to use a registry to get your image into the cluster.
`pachctl` can help you with this by way of the `--push-images` flag which is
accepted by `create-pipeline` and `update-pipeline`. By default the flag will
attempt to push the image to a registry that's started inside of the Kubernetes
cluster when you launch Pachyderm. You can use it with other registries such as
DockerHub and Google Container Registry. Learn more about `--push-images` from
the `create-pipeline` [docs](./pachctl/pachctl_create-pipeline.html).

## Creating a Pipeline

Now that you've got your code and image built, the final step is to add a pipeline manifest to Pachyderm. Pachdyerm pipelines are described using a JSON file. There are four main components to a pipeline: name, transform, parallelism and inputs. Detailed explanations of parameters and how they work can be found in the [pipeline_spec](./pipeline_spec.html). 

Here's a template pipeline:
```json
{
  "pipeline": {
    "name": "my-pipeline"
  },
  "transform": {
    "image": "my-image",
    "cmd": [ "my-binary", "arg1", "arg2"],
    "stdin": [
        "my-std-input"
    ]
  },
  "parallelism": "4",
  "inputs": [
    {
      "repo": {
        "name": "my-input"
      },
      "method": "map"
    }
  ]
}
```

After you create the JSON manifest, you can add the pipeline to Pachyderm using:

```
 $ pachctl create-pipeline -f your_pipeline.json
```
`-f` can also take a URL if your JSON manifest is hosted on GitHub for instance. Keeping pipeline manifests under version control too is a great idea so you can track changes and seamlessly view or deploy older pipelines if needed.

Creating a pipeline tells Pachyderm to run your code on *every* finished
commit in the input repo(s) as well as *all future commits* that happen after the pipeline is created. 

You can think of this pipeline as being "subscribed" to any new commits that are made on any of its input repos. It will automatically process the new data as it comes in. 



