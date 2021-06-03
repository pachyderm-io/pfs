package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	zero int64
)

// GetKubeClient connects to the Kubernetes API server either from inside the
// cluster or from a test binary running on a machine with kubectl (it will
// connect to the same cluster as kubectl)
func GetKubeClient(t testing.TB) *kube.Clientset {
	var config *rest.Config
	var err error
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host != "" {
		config, err = rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules,
			&clientcmd.ConfigOverrides{})
		config, err = kubeConfig.ClientConfig()
	}
	require.NoError(t, err)
	k, err := kube.NewForConfig(config)
	require.NoError(t, err)
	return k
}

// DeletePachdPod deletes the pachd pod in a test cluster (restarting it, e.g.
// to retart the PPS master)
func DeletePachdPod(t testing.TB) {
	kubeClient := GetKubeClient(t)
	ctx := context.TODO()
	podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(ctx,
		metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"app": "pachd", "suite": "pachyderm"},
			)),
		})
	require.NoError(t, err)
	require.Equal(t, 1, len(podList.Items))
	require.NoError(t, kubeClient.CoreV1().Pods(v1.NamespaceDefault).Delete(ctx,
		podList.Items[0].ObjectMeta.Name, metav1.DeleteOptions{}))

	// Make sure pachd goes down
	startTime := time.Now()
	require.NoError(t, backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(ctx,
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return nil
		}
		if time.Since(startTime) > 10*time.Second {
			return nil
		}
		return errors.Errorf("waiting for old pachd pod to be killed")
	}, backoff.NewTestingBackOff()))

	// Make sure pachd comes back up
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(ctx,
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return errors.Errorf("no pachd pod up yet")
		}
		return nil
	})

	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(ctx,
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return errors.Errorf("no pachd pod up yet")
		}
		if podList.Items[0].Status.Phase != v1.PodRunning {
			return errors.Errorf("pachd not running yet")
		}
		return err
	})
}

// DeletePipelineRC deletes the RC belonging to the pipeline 'pipeline'. This
// can be used to test PPS's robustness
func DeletePipelineRC(t testing.TB, pipeline string) {
	kubeClient := GetKubeClient(t)
	ctx := context.TODO()
	rcs, err := kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).List(ctx,
		metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"pipelineName": pipeline},
			)),
		})
	require.NoError(t, err)
	require.Equal(t, 1, len(rcs.Items))
	require.NoError(t, kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).Delete(ctx,
		rcs.Items[0].ObjectMeta.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}))
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		rcs, err := kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).List(ctx,
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"pipelineName": pipeline},
				)),
			})
		if err != nil {
			return err
		}
		if len(rcs.Items) != 0 {
			return errors.Errorf("RC %q not deleted yet", pipeline)
		}
		return nil
	})
}

// PachdDeployment finds the corresponding deployment for pachd in the
// kubernetes namespace and returns it.
func PachdDeployment(t testing.TB, namespace string) *apps.Deployment {
	k := GetKubeClient(t)
	ctx := context.TODO()
	result, err := k.AppsV1().Deployments(namespace).Get(ctx, "pachd", metav1.GetOptions{})
	require.NoError(t, err)
	return result
}

func podRunningAndReady(e watch.Event) (bool, error) {
	if e.Type == watch.Deleted {
		return false, errors.New("received DELETE while watching pods")
	}
	pod, ok := e.Object.(*v1.Pod)
	if !ok {
		return false, errors.Errorf("unexpected object type in watch.Event")
	}
	return pod.Status.Phase == v1.PodRunning, nil
}

// WaitForPachdReady finds the pachd pods within the kubernetes namespace and
// blocks until they are all ready.
func WaitForPachdReady(t testing.TB, namespace string) {
	k := GetKubeClient(t)
	ctx := context.TODO()
	deployment := PachdDeployment(t, namespace)
	for {
		newDeployment, err := k.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		require.NoError(t, err)
		if newDeployment.Status.ObservedGeneration >= deployment.Generation && newDeployment.Status.Replicas == *newDeployment.Spec.Replicas {
			break
		}
		time.Sleep(time.Second * 5)
	}
	watch, err := k.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: "app=pachd",
	})
	defer watch.Stop()
	require.NoError(t, err)
	readyPods := make(map[string]bool)
	for event := range watch.ResultChan() {
		ready, err := podRunningAndReady(event)
		require.NoError(t, err)
		if ready {
			pod, ok := event.Object.(*v1.Pod)
			if !ok {
				t.Fatal("event.Object should be an object")
			}
			readyPods[pod.Name] = true
			if len(readyPods) == int(*deployment.Spec.Replicas) {
				break
			}
		}
	}
}
