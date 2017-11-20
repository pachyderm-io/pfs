# Examples

## OpenCV Edge Detection

This example does edge detection using OpenCV. This is our canonical starter demo. If you haven't used Pachyderm before, start here. We'll get you started running Pachyderm locally in just a few minutes and processing sample log lines.

[Open CV](http://pachyderm.readthedocs.io/en/stable/getting_started/beginner_tutorial.html)

## Word Count (Map/Reduce)

Word count is basically the "hello world" of distributed computation. This example is great for benchmarking in distributed deployments on large swaths of text data.

[Word Count](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/word_count)

## Periodic Ingress from a Database

This example pipeline executes a query periodically against a MongoDB database outside of Pachyderm.  The results of the query are stored in a corresponding output repository.  This repository could be used to drive additional pipeline stages periodically based on the results of the query.

[Periodic Ingress from MongoDB](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/cron)

## Variant Calling and Joint Genotyping with GATK

This example illustrates the use of GATK in Pachyderm for Germline variant calling and joint genotyping. Each stage of this GATK best practice pipeline can be scaled individually and is automatically triggered as data flows into the top of the pipeline. The example follows [this tutorial](https://drive.google.com/open?id=0BzI1CyccGsZiQ1BONUxfaGhZRGc) from GATK, which includes more details about the various stages.

[GATK - Variant Calling](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/gatk)

## Machine Learning

### Iris flower classification with R, Python, or Julia

The "hello world" of machine learning implemented in Pachyderm.  You can deploy this pipeline using R, Python, or Julia commponents, where the pipeline includes the trianing of a SVM, LDA, Decision Tree, or Random Forest model and the subsequent utilization of that model to perform inferences.

[R, Python, or Julia - Iris flower classification](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/ml/iris)

### Sentiment analysis with Neon

This example implements the machine learning template pipeline discussed in [this blog post](https://medium.com/pachyderm-data/sustainable-machine-learning-workflows-8c617dd5506d#.hhkbsj1dn).  It trains and utilizes a neural network (implemented in Python using Nervana Neon) to infer the sentiment of movie reviews based on data from IMDB. 

[Neon - Sentiment Analysis](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/ml/neon)

### pix2pix with TensorFlow

If you haven't seen pix2pix, check out [this great demo](https://affinelayer.com/pixsrv/).  In this example, we implement the training and image translation of the pix2pix model in Pachyderm, so you can generate cat images from edge drawings, day time photos from night time photos, etc.

[TensorFlow - pix2pix](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/ml/tensorflow)

### Recurrent Neural Network with Tensorflow

Based on [this Tensorflow example](https://www.tensorflow.org/tutorials/recurrent#recurrent-neural-networks), this pipeline generates a new Game of Thrones script using a model trained on existing Game of Thrones scripts.

[Tensorflow - Recurrent Neural Network](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/ml/rnn) 

### Distributed Hyperparameter Tuning

This example demonstrates how you can evaluate a model or function in a distributed manner on multiple sets of parameters.  In this particular case, we will evaluate many machine learning models, each configured uses different sets of parameters (aka hyperparameters), and we will output only the best performing model or models.

[Hyperparameter Tuning](https://github.com/pachyderm/pachyderm/tree/master/doc/examples/ml/hyperparameter)
