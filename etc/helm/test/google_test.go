// SPDX-FileCopyrightText: Pachyderm, Inc. <info@pachyderm.com>
// SPDX-License-Identifier: Apache-2.0

package helmtest

import (
	"fmt"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	appsV1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storageV1 "k8s.io/api/storage/v1"
)

//Etcd / Pachd Storage Class - Should test  storage class name elsewhere
//Service Account name - Should test setting service account name elsewhere

func TestGoogle(t *testing.T) {

	type envVarMap struct {
		helmKey string
		envVar  string
		value   string
	}
	testCases := []envVarMap{
		{
			helmKey: "pachd.storage.google.bucket",
			value:   "fake-bucket",
			envVar:  "GOOGLE_BUCKET",
		},
		{
			helmKey: "pachd.storage.google.cred",
			value:   `INSERT JSON HERE`,
			envVar:  "GOOGLE_CRED",
		},
	}
	var (
		expectedServiceAccount = "my-fine-sa"
		expectedProvisioner    = "kubernetes.io/gce-pd"
		expectedStorageBackend = "GOOGLE"
	)
	helmValues := map[string]string{
		"deployTarget": expectedStorageBackend,
		`pachd.serviceAccount.additionalAnnotations.iam\.gke\.io/gcp-service-account`:  expectedServiceAccount,
		`worker.serviceAccount.additionalAnnotations.iam\.gke\.io/gcp-service-account`: expectedServiceAccount,
	}
	for _, tc := range testCases {
		helmValues[tc.helmKey] = tc.value
	}

	templatesToCheck := map[string]bool{
		"templates/pachd/storage-secret.yaml":             false,
		"templates/pachd/deployment.yaml":                 false,
		"templates/pachd/rbac/serviceaccount.yaml":        false,
		"templates/pachd/rbac/worker-serviceaccount.yaml": false,
		"templates/etcd/statefulset.yaml":                 false,
		"templates/etcd/storageclass-gcp.yaml":            false,
		"templates/postgresql/statefulset.yaml":           false,
		"templates/postgresql/storageclass-gcp.yaml":      false,
	}

	templatesToRender := []string{}
	for k := range templatesToCheck {
		templatesToRender = append(templatesToRender, k)
	}

	objects, err := manifestToObjects(helm.RenderTemplate(t, &helm.Options{
		SetValues: helmValues,
	}, "../pachyderm", "blah", templatesToRender))

	if err != nil {
		t.Fatal(err)
	}

	for _, object := range objects {
		switch resource := object.(type) {
		case *v1.Secret:
			if resource.Name != "pachyderm-storage-secret" {
				continue
			}

			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s equals %s", tc.envVar, tc.value), func(t *testing.T) {
					if got := string(resource.Data[tc.envVar]); got != tc.value {
						t.Errorf("got %s; want %s", got, tc.value)
					}
				})
			}
			templatesToCheck["templates/pachd/storage-secret.yaml"] = true
		case *v1.ServiceAccount:
			if resource.Name == "pachyderm-worker" || resource.Name == "pachyderm" {

				t.Run(fmt.Sprintf("%s service account annotation equals %s", resource.Name, expectedServiceAccount), func(t *testing.T) {
					if sa := resource.Annotations["iam.gke.io/gcp-service-account"]; sa != expectedServiceAccount {
						t.Errorf("expected service account to be %q but was %q", expectedServiceAccount, sa)
					}
				})
				if resource.Name == "pachyderm-worker" {
					templatesToCheck["templates/pachd/rbac/worker-serviceaccount.yaml"] = true
				}
				if resource.Name == "pachyderm" {
					templatesToCheck["templates/pachd/rbac/serviceaccount.yaml"] = true
				}

			}
		case *appsV1.Deployment:
			if resource.Name != "pachd" {
				continue
			}

			t.Run("pachd deployment env vars", func(t *testing.T) {
				c, ok := GetContainerByName("pachd", resource.Spec.Template.Spec.Containers)
				if !ok {
					t.Errorf("pachd container not found in pachd deployment")
				}
				if err, got := GetEnvVarByName(c.Env, storageBackendEnvVar); err == nil && got != expectedStorageBackend {
					t.Errorf("expected %s to be %q, not %q", storageBackendEnvVar, expectedStorageBackend, got)
				}
			})
			templatesToCheck["templates/pachd/deployment.yaml"] = true
		case *storageV1.StorageClass:
			if resource.Name == "postgresql-storage-class" || resource.Name == "etcd-storage-class" {

				t.Run(fmt.Sprintf("%s storage class annotation equals %s", resource.Name, expectedProvisioner), func(t *testing.T) {
					if resource.Provisioner != expectedProvisioner {
						t.Errorf("expected storageclass provisioner to be %q but it was %q", expectedProvisioner, resource.Provisioner)
					}
				})

				if resource.Name == "postgresql-storage-class" {
					templatesToCheck["templates/postgresql/storageclass-gcp.yaml"] = true
				}
				if resource.Name == "etcd-storage-class" {
					templatesToCheck["templates/etcd/storageclass-gcp.yaml"] = true
				}
			}
		case *appsV1.StatefulSet:
			if resource.Name == "etcd" || resource.Name == "postgres" {

				for _, pvc := range resource.Spec.VolumeClaimTemplates {
					// Check Google Default Storage Request
					expectedStorageSize := "50Gi"

					if pvc.Name == "etcd-storage" {
						t.Run(fmt.Sprintf("%s storage class storage resource request equals %s", resource.Name, expectedStorageSize), func(t *testing.T) {
							if got := pvc.Spec.Resources.Requests.Storage().String(); got != expectedStorageSize {
								t.Errorf("expected stateful set storage resource request to be %q but it was %q", expectedStorageSize, got)

							}
						})
						templatesToCheck["templates/etcd/statefulset.yaml"] = true
					}

					if pvc.Name == "postgres-storage" {
						t.Run(fmt.Sprintf("%s storage class storage resource request equals %s", resource.Name, expectedStorageSize), func(t *testing.T) {
							if got := pvc.Spec.Resources.Requests.Storage().String(); got != expectedStorageSize {
								t.Errorf("expected stateful set storage resource request to be %q but it was %q", expectedStorageSize, got)

							}
						})
						templatesToCheck["templates/postgresql/statefulset.yaml"] = true

					}

				}
			}
		}
	}

	for k, ok := range templatesToCheck {
		if !ok {
			t.Errorf("template %q not checked", k)
		}
	}
}
