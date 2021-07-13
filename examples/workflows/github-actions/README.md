# GitHub Actions (GHA)

In this example, the  user is shown how to use GHA to 
* manually trigger a pipeline build
* automatically build a pipeline from a commit or pull request.

This takes the user from individual contributor to team developer
in a pretty straight line,
with "triggering on a commit" the shadow zone between IC and team.

The example will be a simple two-stage DAG
using a single image for both pipelines that contains two scripts. 
The transform in each stage will call a different script.

Both the code for building the image and the pipeline definitions 
will be in this repository.

The instructions will cover using this as a template, 
copying the directory over to another area
and modifying for project use.

## Preparation

The ones that refer to other existing docs or examples are marked with a *.

1. Ingress to your cluster so that GHA can reach it*
2. Activating access controls & ssl on Pachyderm*
3. Creating a DockerHub (DH) token so GHA can reach it*

Things they should understand

1. IC: Basics of how to use git*
2. Team: what a pull request is*

## Manually triggering a pipeline build

Creating a manual workflow target that builds images and pushes them to your cluster.
This uses the 2000 hr/month credit
that free accounts receive on GH
to run docker build, 
push your image to DH,
then push an updated pipeline definition to Pachyderm.


## Triggering a pipeline build on commit

Use a GHA that triggers when a commit is made.

A bit of discussion on how to use this to automate testing:
this stage can be kept and repurposed to build a test pipeline
for the team workflow stage.

This is where we transition from IC->team. 

"Breaking the build".

## Triggering a pipeline build on PR approval

Full team development, only builds when the PR is approved.




