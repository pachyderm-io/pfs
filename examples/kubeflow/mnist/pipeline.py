#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import tempfile

import kfp
from kfp.compiler import compiler
import kfp.dsl
import kfp.components
from kubernetes.client.models import V1EnvVar

def mnist(s3_endpoint: str, input_bucket: str):
    # install boto3 - kinda nasty, but this is the way to do it in kubeflow's
    # lightweight components
    import sys, subprocess
    subprocess.run([sys.executable, '-m', 'pip', 'install', 'boto3'])

    # imports are done here because it's required for kubeflow's lightweight
    # components:
    # https://www.kubeflow.org/docs/pipelines/sdk/lightweight-python-components/
    import logging
    import os
    import sys
    import tempfile

    import boto3
    import tensorflow as tf
    from tensorflow import keras

    s3_client = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id='',
        aws_secret_access_key=''
    )

    with tempfile.TemporaryDirectory(suffix="pachyderm-mnist") as data_dir:
        # first, we copy files from pachyderm into a convenient
        # local directory for processing.
        training_data_path = os.path.join(data_dir, "mnist.npz")
        logging.info("copying from s3://mnist.npz to {}".format(training_data_path))
        s3_client.download_file(input_bucket, "mnist.npz", training_data_path)

        (train_images, train_labels), (test_images, test_labels) = tf.keras.datasets.mnist.load_data(path=training_data_path)
        train_labels = train_labels[:1000]
        test_labels = test_labels[:1000]

        train_images = train_images[:1000].reshape(-1, 28 * 28) / 255.0
        test_images = test_images[:1000].reshape(-1, 28 * 28) / 255.0

        # Create a basic model instance
        model = tf.keras.models.Sequential([
            keras.layers.Dense(512, activation=tf.keras.activations.relu, input_shape=(784,)),
            keras.layers.Dropout(0.2),
            keras.layers.Dense(10, activation=tf.keras.activations.softmax)
        ])

        model.compile(
            optimizer=tf.keras.optimizers.Adam(),
            loss=tf.keras.losses.sparse_categorical_crossentropy,
            metrics=['accuracy']
        )

        model.summary()

        model.fit(train_images, train_labels, batch_size=32, epochs=5, validation_data=(test_images, test_labels))

        # Save entire model to a HDF5 file
        model_path = os.path.join(data_dir, "my_model.h5")
        model.save(model_path)
        # Copy file over to Pachyderm
        logging.info("copying {} to s3g".format(model_path))
        s3_client.upload_file(model_path, "out", "my_model.h5")

@kfp.dsl.pipeline(
    name="mnist kubeflow pipeline",
    description="Train neural net on MNIST"
)
def kubeflow_pipeline(s3_endpoint: str, input_bucket: str):
    op = kfp.components.func_to_container_op(
        mnist,
        base_image='tensorflow/tensorflow:1.14.0-py3'
    )

    return op(s3_endpoint, input_bucket)


def pipeline_id(client: kfp.Client, name: str):
    """Gets the ID of the kubeflow pipeline with the name 'name'
    Args:
      name of the pipeline
    Returns:
      id of the pipeline
    """
    page_token = ""
    while page_token is not None:
        p = client.list_pipelines(page_token=page_token, page_size=100)
        if p.pipelines is None:
            return ""
        for p in p.pipelines:
            if p.name == name:
                return p.id
        page_token = p.next_page_token
    return ""

def experiment_id(client: kfp.Client, name: str):
    """Gets the ID of the kubeflow experiment with the name 'name'
    Args:
      name of the experiment
    Returns:
      id of the experiment
    """
    page_token = ""
    while page_token is not None:
        p = client.list_experiments(page_token=page_token, page_size=100)
        if p.experiments is None:
            return ""
        for p in p.experiments:
            if p.name == name:
                return p.id
        page_token = p.next_page_token
    return ""

def main(host: str, create_pipeline: str, create_run_in: str, force: bool):
    if create_pipeline != "" and create_run_in != "":
        logging.error('only one of --create-run-in and --create-pipeline may be set')
        return 1

    if host != "":
        client = kfp.Client(host=host)
    else:
        client = kfp.Client()

    run_id = ""
    if create_run_in != "":
        logging.info('creating run in pipeline "{}"'.format(create_run_in))
        pid = pipeline_id(client, create_run_in)
        if pid == "":
            logging.error('could not find pipeline "{}" to create job'.format(
                create_run_in))
            sys.exit(1)
        # Create a run in the target pipeline using the new pipeline ID
        run_info = client.run_pipeline(
            job_name="pach-job-{}".format(os.environ["PACH_JOB_ID"]),
            pipeline_id=pid,
            experiment_id=experiment_id(client, "Default"),
            params = {
                "s3_endpoint": os.environ["S3_ENDPOINT"],
                "input_bucket": "input",
            }
        )
        run_id = run_info.id
    elif create_pipeline != "":
        # Local machine is just creating the pipeline
        with tempfile.NamedTemporaryFile(suffix='.zip') as pipeline_file:
            compiler.Compiler().compile(kubeflow_pipeline, pipeline_file.name)
            pid = pipeline_id(client, create_pipeline)
            if pid != "":
                client.delete_pipeline(pid)
            logging.info("creating pipeline: {}".format(create_pipeline))
            try:
                client.upload_pipeline(pipeline_file.name, create_pipeline)
            except TypeError:
                pass # https://github.com/kubeflow/pipelines/issues/2764
                     # This can be removed once KF proper uses the latest KFP
    else:
        # Pachyderm job is creating both the pipeline and the run
        run_id = client.create_run_from_pipeline_func(
            kubeflow_pipeline,
            run_name="pach-job-{}".format(os.environ["PACH_JOB_ID"]),
            arguments={
                "s3_endpoint": os.environ["S3_ENDPOINT"],
                "input_bucket": "input",
            },
            namespace="anonymous"
        ).run_id

    if run_id != "":
        logging.info("waiting on kubeflow run id: {}".format(run_id))
        j = client.wait_for_run_completion(run_id, 60)
        assert j.run.status == 'Succeeded'
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a kubeflow pipeline ' +
        'that trains a small neural net on MNIST data')
    parser.add_argument('--remote-host',
                        default="", metavar="http://host/path", type=str,
                        help="""The address of the kubeflow pipelines API, if
                             running outside a kubeflow cluster""")
    parser.add_argument('--create-run-in',
                        default="", metavar="pipeline_name", type=str,
                        help="""If set, this script will create a run for the
                        given kubeflow pipeline and wait for it to complete,
                        rather than uploading a new pipeline""")
    parser.add_argument('--create-pipeline',
                        default="", metavar="pipeline_name", type=str,
                        help="""If set, this script will create a kubeflow
                        pipeline with the given name, but will not create a run
                        in that pipeline. Used to create a pipeline from a local
                        machine.""")
    parser.add_argument('--force', action='store_true', default=False,
                        help="""If set, and this script tries to create a
                        pipeline where a kubeflow pipeline with the same name
                        already exists, the existing pipeline will be
                        deleted""")
    args = parser.parse_args()
    sys.exit(main(args.remote_host, args.create_pipeline, args.create_run_in, args.force))
