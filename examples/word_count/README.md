# Pachyderm Word Count

In this guide, we will write a classic [word count](https://portal.futuresystems.org/manual/hadoop-wordcount) application on Pachyderm.  This is a somewhat advanced guide; to learn the basic usage of Pachyderm, start with the [beginner tutorial](https://docs.pachyderm.com/latest/getting_started/beginner_tutorial/).

## Setup

This guide assumes that you already have a Pachyderm cluster running and have configured `pachctl` to talk to the cluster. See [Installation instructions](https://docs.pachyderm.com/latest/getting_started/local_installation/).

## Pipelines

In this example, we will have three processing stages defined by three pipeline stages:

![alt text](pachyderm_word_count.png)

Our first pipeline, `scraper`, is a web scraper that just pulls content from the internet. Our second pipeline, `map`, tokenizes the words from the scraped pages in parallel over all pages and appends counts of words to files corresponding to those words. Our final pipeline, `reduce`, aggregates the total counts for each word. 

All three pipelines, including `reduce`, can be run in a distributed fashion to maximize performance. 

## Input

Our input data is a set of files. Each file is named for the site we want to scrape with the content being the URL or URLs for that site. 

Let's create the input repo and add one URL, Wikipedia:
```shell
$ pachctl create repo urls

# We assume you're running this from the root of this example (pachyderm/examples/word_count/):
$ pachctl put file urls@master -f Wikipedia
```

Then to actually scrape this site and save the data, we create the first pipeline based on the [scraper.json](scraper.json) pipeline specification:

```shell
$ pachctl create pipeline -f scraper.json
```

This first pipeline, `scraper`, uses `wget` to download web pages from Wikipedia which will be used as the input for the next pipeline. It'll take a minute or two because it needs to `apt-get` a few dependencies (this can be avoided by creating a custom Docker container with the dependencies already downloaded).

When you create the `scraper` pipeline, you should be able to see a job running and a new repo called `scraper` that contains the output of our scrape:

```shell
$ pachctl list job
ID                                   OUTPUT COMMIT STARTED       DURATION RESTART PROGRESS STATE            
44190a81-a87b-4a6b-8f25-8e5d3504566a scraper/-     3 seconds ago -        0       0 / 1    running 
$ pachctl list job
ID                                   OUTPUT COMMIT                            STARTED            DURATION   RESTART PROGRESS STATE            
44190a81-a87b-4a6b-8f25-8e5d3504566a scraper/da0786abd4254ff6b2297aeaf10204e4 About a minute ago 42 seconds 0       1 / 1    success 
$ pachctl list repo
NAME                CREATED              SIZE                
scraper             About a minute ago   71.34 KiB           
urls                3 minutes ago        39 B                
$ pachctl list file scraper@master
NAME                TYPE                SIZE                
Wikipedia           dir                 71.34 KiB           
$ pachctl list file scraper@master:Wikipedia
NAME                       TYPE                SIZE                
Wikipedia/Main_Page.html   file                71.34 KiB
```

## Map

The `map` pipeline counts the number of occurrences of each word it encounters for each of the scraped webpages.  While this task can very well be accomplished in bash, we will demonstrate how to use custom code in Pachyderm by using a [Go program](map/map.go).

In this case, you don't have to build a custom Docker image yourself with this compiled program. We have pushed a public image to Docker Hub, `pachyderm/wordcount-map`, which is referenced in the [map/map.json](map.json) pipeline specification.

Let's create the `map` pipeline: 

```shell
$ pachctl create pipeline -f map/map.json
```

As soon as you create this pipeline, it will start processing data from the `scraper` data repository. For each web page the `map.go` code processes, it writes a file for each encountered word. In our case, the filename for each word is the name of the word itself. To see what I mean, lets run a `pachctl list file` on the map repo:

```shell
$ pachctl list file map@master
NAME          TYPE SIZE
a             file 4B
ability       file 2B
about         file 3B
aboutsite     file 2B
absolute      file 3B
accesskey     file 3B
account       file 2B
acnh          file 2B
action        file 3B
actions       file 2B
activities    file 2B
actor         file 2B
...
```
As you can see, for every word on that page there is a seperate file. Inside that file is the numeric value for how many times that word appeared. You can do a `get file` on say the "about" file to see how many times that word shows up in our scrape:

```shell
$ pachctl get file map@master:about
13

```

By default, Pachyderm will spin up the same number of workers as the number of nodes in your cluster.  This can of course be customized or changed as described in [Distributed Computing](https://docs.pachyderm.com/latest/concepts/pipeline-concepts/distributed_computing/#controlling-the-number-of-workers) for more info on controlling the number of workers).

## Reduce

The final pipeline, `reduce` goes through every file and adds up the numbers in each file, thus obtaining a total count per word.  For this pipeline we can use a simple bash script:

```shell
find /pfs/map -name '*' | while read count; do cat $count | awk '{ sum+=$1} END {print sum}' >/tmp/count; mv /tmp/count /pfs/out/`basename $count`; done
```

We have baked this into [reduce.json](reduce.json).  Again, creating the pipeline is as simple as:

```shell
$ pachctl create pipeline -f reduce.json
```

The output should look like:

```shell
$ pachctl list repo
NAME                CREATED             SIZE                
reduce              43 minutes ago      4.216 KiB           
map                 46 minutes ago      2.867 KiB           
scraper             50 minutes ago      71.34 KiB           
urls                53 minutes ago      39 B                
$ pachctl get file reduce@master:wikipedia
241
```

To get a complete list of the words counted:

```shell
$ pachctl list file reduce@master
NAME                                   TYPE                SIZE                
a                                      file                4 B                 
abdul                                  file                2 B                 
about                                  file                3 B                 
aboutsite                              file                2 B                 
absolute                               file                2 B                 
accesskey                              file                3 B                 
accidentally                           file                2 B                 
account                                file                2 B                 
across                                 file                2 B                 
action                                 file                2 B                 
activities                             file                2 B                 
additional                             file                2 B 

etc...
```

## Expand on the example

Now that we've got a full end-to-end scraper and wordcount use case set up, lets add more to it. First, let's add more data. Go ahead and add a few more sites to scrape. 

```shell
# Instead of using the -c shorthand flag, let's do this the long way by starting a commit, adding files, and then finishing the commit.
$ pachctl start commit urls@master

# Reminder: files added should be named for the website and have the URL as the content. You'll have to create these files.
$ pachctl put file urls@master -f HackerNews
$ pachctl put file urls@master -f Reddit
$ pachctl put file urls@master -f GitHub

$ pachctl finish commit urls@master
```
Your scraper should automatically get started pulling these new sites (it won't rescrape Wikipedia). That will then automatically trigger the `map` and `reduce` pipelines to process the new data and update the word counts for all the sites combined.

If you add a bunch more data and your pipeline starts to run slowly, you can crank up the parallelism. By default, pipelines spin up one worker for each node in your cluster, but you can set that manually with the [parallelism spec](https://docs.pachyderm.com/latest/concepts/pipeline-concepts/distributed_computing/#controlling-the-number-of-workers) field in the pipeline specification. Further, the pipelines are already configured to spread computation across the various workers with `"glob": "/*"`. Check out our [Glob Pattern](https://docs.pachyderm.com/latest/concepts/pipeline-concepts/datum/glob-pattern/) to learn more.


