
# Set as you wish
DOCKER_ACCOUNT := pachyderm
CONTAINER_NAME := example-rabbitmq-spout
CONTAINER_VERSION := 1.12.2
CONTAINER_TAG := $(DOCKER_ACCOUNT)/$(CONTAINER_NAME):$(CONTAINER_VERSION)

docker-image:
	@docker build -t $(CONTAINER_TAG) .
	@docker push $(CONTAINER_TAG)

create-pipeline:
	pachctl create pipeline -f pipelines/spout.pipeline.json

update-pipeline:
	pachctl update pipeline -f pipelines/spout.pipeline.json

delete-pipeline:
	pachctl delete pipeline -f rabbitmq-spout
