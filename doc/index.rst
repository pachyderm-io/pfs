.. Pachyderm documentation master file, created by
   sphinx-quickstart on Thu Jul  7 10:45:21 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _Go Client: https://godoc.org/github.com/pachyderm/pachyderm/src/client

Pachyderm Developer Documentation
=================================

Welcome to the Pachyderm documentation portal!  Below you'll find guides and information for beginners and experienced Pachyderm users. You'll also find API references docs. 

If you can't find what you're looking for or have a an issue not mentioned here, we'd love to hear from you either on `GitHub <https://github.com/pachyderm/pachyderm>`_, our `Users Slack channel <http://slack.pachyderm.io/>`_, or email us at support@pachyderm.io. 

Note: if you are using a Pachyderm version < 1.4, you can find relevant docs `here <http://docs.pachyderm.io/en/v1.3.18/>`_.

.. toctree::
    :maxdepth: 1
    :caption: Getting Started

    getting_started/getting_started
    getting_started/local_installation
    getting_started/beginner_tutorial

.. toctree::
    :maxdepth: 1
    :caption: Pachyderm Fundamentals

    fundamentals/getting_data_into_pachyderm
    fundamentals/creating_analysis_pipelines
    fundamentals/getting_data_out_of_pachyderm
    fundamentals/removing_data_from_pachyderm
    fundamentals/append_overwrite
    fundamentals/lifecycle_of_a_datum
    fundamentals/updating_pipelines
    fundamentals/distributed_computing
    fundamentals/incrementality
    fundamentals/spouts

.. toctree::
    :maxdepth: 1
    :caption: Pachyderm Enterprise Edition

    enterprise/overview
    enterprise/deployment
    enterprise/auth
    enterprise/stats
    enterprise/s3gateway

.. toctree::
    :maxdepth: 1
    :caption: Deploy Pachyderm

    deployment/deploy_intro
    deployment/google_cloud_platform
    deployment/amazon_web_services
    deployment/azure
    deployment/openshift
    deployment/on_premises
    deployment/custom_object_stores
    deployment/aws_cloudfront
    deployment/upgrading
    deployment/namespaces
    deployment/rbac
    deployment/deploy_troubleshooting

.. toctree::
    :maxdepth: 1
    :caption: Manage Pachyderm

    managing_pachyderm/autoscaling
    managing_pachyderm/data_management
    managing_pachyderm/sharing_gpu_resources
    managing_pachyderm/backup_restore
    managing_pachyderm/upgrades_migrations
    managing_pachyderm/general_troubleshooting
    managing_pachyderm/pipeline_troubleshooting

.. toctree::
    :maxdepth: 1
    :caption: Full Examples

    examples/examples
    
.. toctree::
    :maxdepth: 1
    :caption: Pachyderm Cookbook

    cookbook/splitting
    cookbook/combining
    cookbook/example_developer_workflow
    cookbook/cron
    cookbook/ml
    cookbook/time_windows
    cookbook/ingressing_from_diff_cloud
    cookbook/gpus
    cookbook/deferred_processing
    cookbook/vault
 
.. toctree::
    :maxdepth: 2
    :caption: Reference

    reference/pipeline_spec
    pachctl/pachctl
    reference/clients
    reference/s3gateway_api
    


