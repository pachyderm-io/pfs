# Helm Deployment

The package manager [Helm](https://helm.sh/docs/intro/install/#helm) is the authoritative deployment method for Pachyderm.

!!! Reminder
    For improved security, **Pachyderm services are now exposed on the cluster internal IP (ClusterIP) instead of each node’s IP (Nodeport)**. These changes do not apply to LOCAL Helm installations (i.e. Services are still accessible through Nodeports on Local installations)

This page gives you a high level view of the steps to follow to install Pachyderm using Helm. Find our chart on [Artifacthub](https://artifacthub.io/packages/helm/pachyderm/pachyderm) or in our [GitHub repository](https://github.com/pachyderm/helmchart).

## Install
### Prerequisites
1. Install [`Helm`](https://helm.sh/docs/intro/install/). 

1. Choose the deployment [guidelines](https://docs.pachyderm.com/latest/deploy-manage/deploy/) that apply to you:
    * **Find the deployment page that applies to your Cloud provider** (or custom deployment, or on-premises deployment).
    It will help list the various installation prerequisites, Kubernetes deployment instructions, and kubectl installation that fit your own use case:
    
        For example, if your Cloud provider is Google Cloud Platform, follow the **Prerequisites** and **Deploy Kubernetes** sections of the [deployment on Google Cloud Platform](https://docs.pachyderm.com/latest/deploy-manage/deploy/google_cloud_platform/#google-cloud-platform) page.

    * Additionally, those instructions will also help you configure the various elements (object store, credentials...) that relate to your deployment needs. Those parameters values will **be specified in a YAML configuration file** as follows.

### Edit a values.yaml file
Create a personalized `my_pachyderm_values.yaml` out of this [example repository](https://github.com/pachyderm/helmchart/tree/v2.0.x/examples). Pick the example that fits your target deployment and update the relevant fields according to the parameters gathered in the previous step.   

See the reference [values.yaml](https://github.com/pachyderm/helmchart/blob/v2.0.x/pachyderm/values.yaml) for an exhaustive list of all parameters.

!!! Warning
    **No default k8s CPU and memory requests and limits** are created for pachd.  If you don't provide values in the values.yaml file, then those requests and limits are simply not set. 
    
    For Production deployments, Pachyderm strongly recommends that you **[create your values.yaml file with CPU and memory requests and limits for both pachd and etcd](https://github.com/pachyderm/helmchart/blob/v2.0.x/pachyderm/values.yaml#L30)** set to values appropriate to your specific environment. For reference, 1 CPU and 2 GB memory for each is a sensible default.    
###  Install the Pachyderm Helm Chart
1. Get your Helm Repo Info
    ```shell
    $ helm repo add pachyderm https://pachyderm.github.io/helmchart
    $ helm repo update
    ```

1. Install Pachyderm

    You are ready to deploy Pachyderm on the environment of your choice.
    ```shell
    $ helm install pachd -f my_pachyderm_values.yaml pach/pachyderm
    ```
!!! Info
    You can choose a specific helm chart version by adding a `--version` flag (for example, `--version 0.3.0`). 
    **Each version of a chart is associated with a given version of Pachyderm**. No mention of the version will install the latest available version of Pachyderm by default. 
    [Artifacthub](https://artifacthub.io/packages/helm/pachyderm/pachyderm) lists all available chart versions and their associated version of Pachyderm. 

### Check your installation

1. Check your deployment
    ```shell
    $ kubectl get pods
    ```

    **System Response:**

    ```
    NAME                           READY   STATUS    RESTARTS   AGE
    etcd-0                         1/1     Running   0          18h
    pachd-5db79fb9dd-b2gdq         1/1     Running   2          18h
    postgres-0                     1/1     Running   0          18h
    ```

1. Verify that the Pachyderm cluster is up and running

    ```shell
    $ pachctl version
    ```

    **System Response:**

    ```
    COMPONENT           VERSION
    pachctl             {{ config.pach_latest_version }}
    pachd               {{ config.pach_latest_version }}
    ```

## Uninstall the Pachyderm Helm Chart
[Helm uninstall](https://helm.sh/docs/helm/helm_uninstall/) a release as easily as you installed it.
```shell
$ helm uninstall pachd 
```
