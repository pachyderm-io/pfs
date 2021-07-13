# Deploy Pachyderm on Amazon EKS

Amazon EKS provides an easy way to deploy, configure, and
manage Kubernetes clusters. If you want to avoid managing your
Kubernetes infrastructure, EKS might be
the right choice for your organization. Pachyderm seamlessly
deploys on Amazon EKS.

## Prerequisites

Before you can deploy Pachyderm on an EKS cluster, verify that
you have the following prerequisites installed and configured:

* [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* [AWS CLI](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html)
* [eksctl](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html)
* [aws-iam-authenticator](https://docs.aws.amazon.com/eks/latest/userguide/install-aws-iam-authenticator.html).
* [pachctl]()

## Deploy an EKS cluster by using `eksctl`

Use the `eksctl` tool to deploy an EKS cluster in your
Amazon AWS environment. The `eksctl create cluster` command
creates a virtual private cloud (VPC), a security group,
and an IAM role for Kubernetes to create resources.
For detailed instructions, see [Amazon documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html).

To deploy an EKS cluster, complete the following steps:

1. Deploy an EKS cluster:

      ```shell
      eksctl create cluster --name <name> --version <version> \
      --nodegroup-name <name> --node-type <vm-flavor> \
      --nodes <number-of-nodes> --nodes-min <min-number-nodes> \
      --nodes-max <max-number-nodes> --node-ami auto
      ```

      **Example**

      ```shell
      eksctl create cluster --name pachyderm-cluster --region us-east-2 --profile <your named profile>
      ```

1. Verify the deployment:

      ```shell
      kubectl get all
      ```

      **System Response:**

      ```
      NAME                 TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)   AGE
      service/kubernetes   ClusterIP   10.100.0.1   <none>        443/TCP   23h
      ```

1. Deploy Pachyderm as described in [Deploy Pachyderm on AWS](./aws-deploy-pachyderm.md).

