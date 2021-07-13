# Deploy Pachyderm on AWS

Once your Kubernetes cluster is up,
you are ready to deploy Pachyderm.

Complete the following steps:

1. [Deploy Amazon EBS CSI driver to your cluster](1-deploy-amazon-EBS-CSI-driver-to-your-cluster)
1. [Create an S3 bucket](#2.1-create-an-S3-object-store-bucket-for-data) for Pachyderm
1. [Deploy Pachyderm ](#3-deploy-pachyderm)
1. Finally, you will need to install [pachctl](../../../../getting_started/local_installation#install-pachctl) to [interact with your cluster]((#have-pachctl-and-your-cluster-communicate)).

## 1- Deploy Amazon EBS CSI driver to your cluster

For metadata storage, etcd and PostgreSQL each claim the creation of a pv. 
For your EKS cluster to successfully create two **Elastic Block Storage (EBS) persistent volumes (PV)**, follow the steps detailled in **[deploy Amazon EBS CSI driver to your cluster](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html)**.

In short, you will:

1. [Create an IAM OIDC provider for your cluster](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html).
1. Create a CSI Driver service account whose IAM Role will be granted the permission (policy) to make calls to AWS APIs. 
1. Install Amazon EBS Container Storage Interface (CSI) driver on your cluster configured with your created service account.


!!! Warning
      The metadata services generally require a small persistent volume size (i.e. 10GB) **but high IOPS (1500)**. Pachyderm recommends SSD **gp3** for these persistent EBS volume which delivers a baseline performance of 3,000 IOPS and 125MB/s at any volume size. Any other disk choice may require to oversize the volume significantly to ensure enough IOPS.


See [volume types](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html).

If you expect your cluster to be very long
running or scale to thousands of jobs per commits, you might need to add
more storage.  However, you can easily increase the size of the persistent
volume later.

## 2- Create an S3 bucket
### Create an **S3 object store bucket for data**
The S3 bucket name
must be globally unique across the whole
Amazon region. Therefore, add a descriptive prefix to the S3 bucket
name, such as your username.

* Set up the following system variables:

      * `BUCKET_NAME` — A globally unique S3 bucket name.
      * `AWS_REGION` — The AWS region of your Kubernetes cluster. For example,
      `us-west-2` and not `us-west-2a`.

* If you are creating an S3 bucket in the `us-east-1` region, run the following
      command:

      ```shell
      $ aws s3api create-bucket --bucket ${BUCKET_NAME} --region ${AWS_REGION}
      ```

* If you are creating an S3 bucket in any region but the `us-east-1`
region, run the following command:

      ```shell
      $ aws s3api create-bucket --bucket ${BUCKET_NAME} --region ${AWS_REGION} --create-bucket-configuration LocationConstraint=${AWS_REGION}
      ```

* Verify that the S3 bucket was created:

      ```shell   
      $ aws s3 ls
      ```

### (Optional) Set up Bucket Encryption

Amazon S3 supports two types of bucket encryption — server-side encryption
(SSE-S3) and AWS Key Management Service (AWS KMS), which stores customer
master keys. When creating a bucket for your Pachyderm cluster, you can set up either
of them. Because Pachyderm requests that buckets do not include encryption
information, the method that you select for the bucket is applied.

!!! Info
      Setting up communication between Pachyderm object storage clients and AWS KMS
      to append encryption information to Pachyderm requests is not supported and
      not recommended. 

To set up bucket encryption, see [Amazon S3 Default Encryption for S3 Buckets](https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html).

## 3- Deploy Pachyderm
You have configure your EKS cluster to create your pvs (metadata) 
and have created your S3 bucket.

You now need to give your cluster access to your bucket either by:

- adding a policy to your cluster IAM Role (Recommended)
- passing your AWS credentials (account ID and KEY) to your values.yaml when installing

!!! Info
      IAM roles provide finer grained user management and security
      capabilities than access keys. Pachyderm recommends the use of IAM roles for production
      deployments.

### Add a policy to your EKS IAM Role

Make sure the **IAM role of your cluster has access to the S3 bucket that you created for Pachyderm**. 

1. Find the IAM role assigned to the cluster:

      1. Go to the AWS Management console.
      1. Select your cluster instance in **Amazon EKS**.
      1. In the general **Details** tab, find your **Cluster IAM Role ARN**.
      1. Find the **IAM Role** field.

1. **Enable access to the S3 bucket** for the IAM role:

      1. Click on the **IAM Role**.
      1. In the **Permissions** tab, click **Add inline policy**.
      1. Select the **JSON** tab.
      1. Copy/Paste the following text in the JSON tab:

      ```json
      {
      "Version": "2012-10-17",
      "Statement": [
            {
      "Effect": "Allow",
            "Action": [
                  "s3:ListBucket"
            ],
            "Resource": [
                  "arn:aws:s3:::<your-bucket>"
            ]},{
      "Effect": "Allow",
      "Action": [
            "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject"
      ],
      "Resource": [
            "arn:aws:s3:::<your-bucket>/*"
      ]}
      ]}
      ```

      Replace `<your-bucket>` with the name of your S3 bucket.

1. Create a name for the new policy.


### Create your values.yaml   

Update your values.yaml with your bucket name ([see example of values.yaml here](https://github.com/pachyderm/helmchart/blob/v2.0.x/examples/gcp-values.yaml)) or use our minimal example below.


=== "values.yaml with an added policy to your cluster IAM Role"
      ```yaml
      # SPDX-FileCopyrightText: Pachyderm, Inc. <info@pachyderm.com>
      # SPDX-License-Identifier: Apache-2.0
      pachd:
      storage:
      backend: AMAZON
      amazon:
            bucket: pachyderm-bucket-02114 
            region: us-east-2
      serviceAccount:
      additionalAnnotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::190146978412:role/eksctl-new-pachyderm-cluster-cluster-ServiceRole-1H3YFIPV75B52

      worker:
      serviceAccount:
      additionalAnnotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::190146978412:role/eksctl-new-pachyderm-cluster-cluster-ServiceRole-1H3YFIPV75B52
      ```
=== "values.yaml passing AWS credentials (account ID and KEY)"
      ```yaml
      # SPDX-FileCopyrightText: Pachyderm, Inc. <info@pachyderm.com>
      # SPDX-License-Identifier: Apache-2.0
      pachd:
      storage:
      backend: AMAZON
      amazon:
            bucket: pachyderm-bucket-02114 
            region: us-east-2
            id: AKIASYRNEUJWABZTZBF5
            secret: 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
      serviceAccount:
      additionalAnnotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::190146978412:role/eksctl-new-pachyderm-cluster-cluster-ServiceRole-1H3YFIPV75B52

      worker:
      serviceAccount:
      additionalAnnotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::190146978412:role/eksctl-new-pachyderm-cluster-cluster-ServiceRole-1H3YFIPV75B52
      ```

!!! Note
      * The **worker nodes on which Pachyderm is deployed must be associated
      with the IAM role that is assigned to the Kubernetes cluster**.
      If you created your cluster by using `eksctl` or `kops` 
      the nodes must have a dedicated IAM role already assigned.

      * The IAM role of your cluster must have correct trust relationships.

            1. Click the **Trust relationships > Edit trust relationship**.
            1. Append the following statement to your JSON relationship:
                  ```json
                  {
                  "Version": "2012-10-17",
                  "Statement": [
                        {
                        "Effect": "Allow",
                        "Principal": {
                        "Service": "ec2.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                        }
                  ]
                  }
                  ```

### Deploy Pachyderm on the Kubernetes cluster

Refer to our generic ["Helm Install"](./helm_install.md) page for more information on the required installations and modus operandi of an installation using `Helm`.

- Now you can deploy a Pachyderm cluster by running this command:

      ```shell
      $ helm repo add pachyderm https://pachyderm.github.io/helmchart
      $ helm repo update
      $ helm install pachyderm -f my_values.yaml pachyderm/pachyderm --version <version-of-the-chart>

      ```

      **System Response:**

      ```shell


      Pachyderm is launching. Check its status with "kubectl get all"
      ```


      The deployment takes some time. You can run `kubectl get pods` periodically
      to check the status of deployment. When Pachyderm is deployed, the command
      shows all pods as `READY`:

      ```shell
      $ kubectl get pods
      ```

      **System Response**

      ```
      NAME                     READY     STATUS    RESTARTS   AGE
      dash-6c9dc97d9c-89dv9    2/2       Running   0          1m
      etcd-0                   1/1       Running   0          4m
      pachd-65fd68d6d4-8vjq7   1/1       Running   0          4m
      ```

      **Note:** If you see a few restarts on the `pachd` nodes, it means that
      Kubernetes tried to bring up those pods before `etcd` was ready. Therefore,
      Kubernetes restarted those pods. You can safely ignore this message.

- Verify that the Pachyderm cluster is up and running:

      ```shell
      $ pachctl version
      ```

      **System Response:**

      ```shell
      COMPONENT           VERSION
      pachctl             {{ config.pach_latest_version }}
      pachd               {{ config.pach_latest_version }}
      ```
     
- Finally, make sure [`pachtl` talks with your cluster](#4-have-pachctl-and-your-cluster-communicate).

## 4- Have 'pachctl' and your Cluster Communicate

Assuming your `pachd` is running as shown above, 
make sure that `pachctl` can talk to the cluster by either:

- Running a port-forward:

```shell
# Background this process because it blocks.
$ pachctl port-forward   
```

- Exposing your cluster to the internet by setting up a LoadBalancer as follow:

!!! Warning 
      The following setup of a LoadBalancer only applies to pachd.

1. To get an external IP address for a Cluster, edit its k8s service, 
```shell
$ kubectl edit service pachd
```
and change its `spec.type` value from `ClusterIP` to `LoadBalancer`. 

1. Retrieve the external IP address of the edited service.
When listing your services again, you should see an external IP address allocated to the service you just edited. 
```shell
$ kubectl get service
```
1. Update the context of your cluster with their direct url, using the external IP address above:
```shell
$ echo '{"pachd_address": "grpc://<external-IP-address>:650"}' | pachctl config set context "<your-cluster-context-name>" --overwrite
```
1. Check that your are using the right context: 
```shell
$ pachctl config get active-context`
```
Your cluster context name should show up. 
