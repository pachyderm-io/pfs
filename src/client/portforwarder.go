package client

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"

	"github.com/pachyderm/pachyderm/src/client/pkg/config"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // enables support for configs with auth
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// PortForwarder handles proxying local traffic to a kubernetes pod
type PortForwarder struct {
	core          corev1.CoreV1Interface
	client        rest.Interface
	config        *rest.Config
	namespace     string
	logger        *io.PipeWriter
	stopChansLock *sync.Mutex
	stopChans     []chan struct{}
	shutdown      bool
}

// NewPortForwarder creates a new port forwarder
func NewPortForwarder(context *config.Context, namespace string) (*PortForwarder, error) {
	if namespace == "" {
		namespace = context.Namespace
	}
	if namespace == "" {
		namespace = "default"
	}

	kubeConfig := config.KubeConfig(context)

	kubeClientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(kubeClientConfig)
	if err != nil {
		return nil, err
	}

	core := client.CoreV1()

	return &PortForwarder{
		core:          core,
		client:        core.RESTClient(),
		config:        kubeClientConfig,
		namespace:     namespace,
		logger:        log.StandardLogger().Writer(),
		stopChansLock: &sync.Mutex{},
		stopChans:     []chan struct{}{},
		shutdown:      false,
	}, nil
}

// Run starts the port forwarder. Returns after initialization is begun with
// the locally bound port and any initialization errors.
func (f *PortForwarder) Run(appName string, localPort, remotePort uint16) (uint16, error) {
	podNameSelector := map[string]string{
		"suite": "pachyderm",
		"app":   appName,
	}
	ctx := context.TODO()
	podList, err := f.core.Pods(f.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(podNameSelector)),
		TypeMeta: metav1.TypeMeta{
			Kind:       "ListOptions",
			APIVersion: "v1",
		},
	})
	if err != nil {
		return 0, err
	}
	if len(podList.Items) == 0 {
		return 0, errors.Errorf("no pods found for app %s", appName)
	}

	// Choose a random pod
	podName := podList.Items[rand.Intn(len(podList.Items))].Name

	url := f.client.Post().
		Resource("pods").
		Namespace(f.namespace).
		Name(podName).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(f.config)
	if err != nil {
		return 0, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)
	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	readyChan := make(chan struct{}, 1)
	stopChan := make(chan struct{}, 1)

	// Ensure that the port forwarder isn't already shutdown, and append the
	// shutdown channel so this forwarder can be closed
	f.stopChansLock.Lock()
	if f.shutdown {
		f.stopChansLock.Unlock()
		return 0, errors.Errorf("port forwarder is shutdown")
	}
	f.stopChans = append(f.stopChans, stopChan)
	f.stopChansLock.Unlock()

	fw, err := portforward.New(dialer, ports, stopChan, readyChan, ioutil.Discard, f.logger)
	if err != nil {
		return 0, err
	}

	errChan := make(chan error, 1)
	go func() { errChan <- fw.ForwardPorts() }()

	select {
	case err = <-errChan:
		return 0, errors.Wrap(err, "port forwarding failed")
	case <-fw.Ready:
	}

	// don't discover the locally bound port if we already know what it is
	if localPort != 0 {
		return localPort, nil
	}

	// discover the locally bound port if we don't know what it is
	bindings, err := fw.GetPorts()
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch local bound ports")
	}

	for _, binding := range bindings {
		if binding.Remote == remotePort {
			return binding.Local, nil
		}
	}

	return 0, errors.New("failed to discover local bound port")
}

// RunForDaemon creates a port forwarder for the pachd daemon.
func (f *PortForwarder) RunForDaemon(localPort, remotePort uint16) (uint16, error) {
	return f.Run("pachd", localPort, remotePort)
}

// RunForSAMLACS creates a port forwarder for SAML ACS.
func (f *PortForwarder) RunForSAMLACS(localPort, remotePort uint16) (uint16, error) {
	return f.Run("pachd", localPort, remotePort)
}

// RunForOIDCACS creates a port forwarder for OIDC ACS.
func (f *PortForwarder) RunForOIDCACS(localPort, remotePort uint16) (uint16, error) {
	return f.Run("pachd", localPort, remotePort)
}

// RunForDashUI creates a port forwarder for the dash UI.
func (f *PortForwarder) RunForDashUI(localPort uint16) (uint16, error) {
	return f.Run("dash", localPort, 8080)
}

// RunForDashWebSocket creates a port forwarder for the dash websocket.
func (f *PortForwarder) RunForDashWebSocket(localPort uint16) (uint16, error) {
	return f.Run("dash", localPort, 8081)
}

// RunForPFS creates a port forwarder for PFS over HTTP.
func (f *PortForwarder) RunForPFS(localPort uint16) (uint16, error) {
	return f.Run("pachd", localPort, 30652)
}

// RunForS3Gateway creates a port forwarder for the s3gateway.
func (f *PortForwarder) RunForS3Gateway(localPort uint16) (uint16, error) {
	return f.Run("pachd", localPort, 600)
}

// Close shuts down port forwarding.
func (f *PortForwarder) Close() {
	defer f.logger.Close()

	f.stopChansLock.Lock()
	defer f.stopChansLock.Unlock()

	if f.shutdown {
		panic("port forwarder already shutdown")
	}

	f.shutdown = true

	for _, stopChan := range f.stopChans {
		close(stopChan)
	}
}
