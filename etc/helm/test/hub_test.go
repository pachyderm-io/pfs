// SPDX-FileCopyrightText: Pachyderm, Inc. <info@pachyderm.com>
// SPDX-License-Identifier: Apache-2.0

package helmtest

import (
	"log"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	appsV1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/networking/v1beta1"
)

func TestHub(t *testing.T) {
	var (
		objects []interface{}
		checks  = map[string]bool{
			"ingress":                false,
			"metricsEndpoint":        false,
			"console limits":         false,
			"etcd limits":            false,
			"loki logging":           false,
			"postgres host":          false,
			"pachd service type":     false,
			"etcd prometheus port":   false,
			"etcd prometheus scrape": false,
			"etcd storage class":     false,
		}
		err error
	)

	if objects, err = manifestToObjects(helm.RenderTemplate(t,
		&helm.Options{
			ValuesFiles: []string{"../examples/hub-values.yaml"},
		},
		"../pachyderm/", "pachd", nil)); err != nil {
		t.Fatalf("could not render templates to objects: %v", err)
	}
	for _, object := range objects {
		switch object := object.(type) {
		case *v1beta1.Ingress:
			log.Println("ingress", object.String())
			for _, rule := range object.Spec.Rules {
				if rule.Host == "console.test" {
					checks["ingress"] = true
				}
			}
		case *appsV1.Deployment:
			switch object.Name {
			case "pachd":
				for _, cc := range object.Spec.Template.Spec.Containers {
					if cc.Name != "pachd" {
						continue
					}
					for _, v := range cc.Env {
						switch v.Name {
						case "METRICS_ENDPOINT":
							expected := "https://metrics.test/api/v1/metrics"
							if v.Value != expected {
								t.Errorf("metrics endpoint %q ≠ %q", v.Value, expected)
							}
							checks["metricsEndpoint"] = true
						case "LOKI_LOGGING":
							if v.Value != "true" {
								t.Error("Loki logging should be enabled")
							}
							checks["loki logging"] = true
						case "POSTGRES_HOST":
							if v.Value != "169.254.169.254" {
								t.Error("Postgres Host should be set")
							}
							checks["postgres host"] = true
						}
					}
				}
			case "console":
				for _, cc := range object.Spec.Template.Spec.Containers {
					if cc.Name != "console" {
						continue
					}
					if len(cc.Resources.Limits) > 0 {
						t.Errorf("console should have no resource limits")
					}
					checks["console limits"] = true
				}
			}
		case *v1.Secret:
			if object.Name != "console-tls" {
				continue
			}
			t.Errorf("there should be no console-tls secret")
		case *v1.Service:
			switch object.Name {
			case "pachd":
				if object.Spec.Type != "ClusterIP" {
					t.Errorf("pachd service type should be \"ClusterIP\", not %q", object.Spec.Type)
				}
				checks["pachd service type"] = true
			case "etcd":
				for k, v := range object.Annotations {
					switch k {
					case "prometheus.io/port":
						if v != "2379" {
							t.Errorf("Promethus port set to %q instead of 2379", v)
						}
						checks["etcd prometheus port"] = true
					case "prometheus.io/scrape":
						if v != "true" {
							t.Errorf("Prometheus scrape set to %q instead of true", v)
						}
						checks["etcd prometheus scrape"] = true
					}
				}

			case "pachd-lb":
				expectedPorts := map[string]*struct {
					port  int
					found bool
				}{
					"api-grpc-port": {
						port:  31400,
						found: false,
					},
					"api-http-port": {
						port:  30652,
						found: false,
					},
					"s3gateway-port": {
						port:  30600,
						found: false,
					},
				}

				for _, port := range object.Spec.Ports {
					ep, ok := expectedPorts[port.Name]
					if !ok {
						t.Errorf("did not find port %q in expected ports", port.Name)
						continue
					}
					if ep.port != int(port.Port) {
						t.Errorf("wanted %q, for port: %q, Got: %d", ep.port, port.Name, port.Port)
						continue
					}
					ep.found = true
				}

				for portName, check := range expectedPorts {
					if !check.found {
						t.Errorf("expected port: %q, not found", portName)
					}
				}

			}
		case *appsV1.StatefulSet:
			switch object.Name {
			case "etcd":
				for _, pvc := range object.Spec.VolumeClaimTemplates {
					if *pvc.Spec.StorageClassName != "ssd-storage-class" {
						t.Errorf("storage class is %q, not ssd-storage-class", *pvc.Spec.StorageClassName)
					}
					checks["etcd storage class"] = true
				}
				for _, cc := range object.Spec.Template.Spec.Containers {
					if cc.Name != "etcd" {
						continue
					}
					if len(cc.Resources.Limits) > 0 {
						t.Errorf("etcd should have no resource limits")
					}
					checks["etcd limits"] = true
				}
			case "postgres":
				t.Errorf("there should be no postgres statefulset")
			}
		}
	}

	for check := range checks {
		if !checks[check] {
			t.Errorf("%q incomplete", check)
		}
	}
}
