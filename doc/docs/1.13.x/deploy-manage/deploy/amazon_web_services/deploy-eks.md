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

   **Example output:**

   ```shell
   [ℹ]  using region us-east-1
   [ℹ]  setting availability zones to [us-east-1a us-east-1f]
   [ℹ]  subnets for us-east-1a - public:192.168.0.0/19 private:192.168.64.0/19
   [ℹ]  subnets for us-east-1f - public:192.168.32.0/19 private:192.168.96.0/19
   [ℹ]  nodegroup "pachyderm-test-workers" will use "ami-0f2e8e5663e16b436" [AmazonLinux2/1.13]
   [ℹ]  using Kubernetes version 1.13
   [ℹ]  creating EKS cluster "pachyderm-test-eks" in "us-east-1" region
   [ℹ]  will create 2 separate CloudFormation stacks for cluster itself and the initial nodegroup
   [ℹ]  if you encounter any issues, check CloudFormation console or try 'eksctl utils describe-stacks --region=us-east-1 --name=pachyderm-test-eks'
   [ℹ]  2 sequential tasks: { create cluster control plane "svetkars-eks", create nodegroup "pachyderm-test-workers" }
   [ℹ]  building cluster stack "eksctl-pachyderm-test-eks-cluster"
   [ℹ]  deploying stack "eksctl-pachyderm-test-eks-cluster"

   ...
   [✔]  EKS cluster "pachyderm-test" in "us-east-1" region is ready
   ```

1. Verify the deployment:

   ```shell
   kubectl get all
   ```

   **System Response:**

   ```shell
   NAME                 TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)   AGE
   service/kubernetes   ClusterIP   10.100.0.1   <none>        443/TCP   7m9s
   ```

1. Deploy Pachyderm as described in [Deploy Pachyderm on AWS](aws-deploy-pachyderm.md).

