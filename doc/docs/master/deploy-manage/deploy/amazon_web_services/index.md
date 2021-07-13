# Deploy Pachyderm on Amazon AWS

Pachyderm can run in a Kubernetes cluster deployed in Amazon®
Web Services (AWS), whether it is an Elastic Container
Service (EKS) or a Kubernetes cluster deployed directly on
EC2 by using a deployment tool.

Installing Pachyderm on Amazon AWS is a 2 steps process:

1. **Create a Kubernetest cluster**:
 
    * [Deploy a cluster on EKS](./deploy-eks)
        If you already have an Amazon EKS cluster, you can quickly deploy
        Pachyderm on top of it. If you are just starting with Amazon EKS,
        this section guides you through the EKS deployment process.

    * [Deploy a cluster on EC2 using `kops`](./aws-deploy-kubernetes-kops)
        Instead of EKS, you can deploy Kubernetes on AWS EC2 directly by
        using a Kubernetes deployment tool such as `kops`,
        then deploy Pachyderm on that Kubernetes cluster.
        If you deploy a cluster with `kops`, you
        remain responsible for the Kubernetes operations and maintenance.

1. [**Install Pachyderm**](./aws-deploy-pachyderm)

1. Optionnal - [Create a CDN with CloudFront](./aws_cloudfront)

    Use this option in production environments that require
    high throughput and secure data delivery.
