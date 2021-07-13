// SPDX-FileCopyrightText: Pachyderm, Inc. <info@pachyderm.com>
// SPDX-License-Identifier: Apache-2.0

package helmtest

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/networking/v1beta1"
)

func TestEnablePachTLSNoName(t *testing.T) {
	helmChartPath := "../pachyderm"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":      "LOCAL",
			"pachd.tls.enabled": "true",
		},
	}

	_, err := helm.RenderTemplateE(t, options, helmChartPath, "secret", []string{"templates/pachd/deployment.yaml"})
	if err == nil {
		t.Error("Template should error")
	}
}

func TestEnablePachTLSExistingSecret(t *testing.T) {
	expectedSecretName := "blah"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":         "LOCAL",
			"pachd.tls.enabled":    "true",
			"pachd.tls.secretName": "blah",
		},
	}
	templates := []string{"templates/pachd/deployment.yaml"}
	output := helm.RenderTemplate(t, options, "../pachyderm", "blah", templates)

	var deployment *appsv1.Deployment

	helm.UnmarshalK8SYaml(t, output, &deployment)

	pachd, ok := GetContainerByName("pachd", deployment.Spec.Template.Spec.Containers)
	if !ok {
		t.Fatalf("pachd container not found")
	}
	want := v1.VolumeMount{
		Name:      "pachd-tls-cert",
		MountPath: "/pachd-tls-cert",
	}

	if !ensureVolumeMountPresent(want, pachd.VolumeMounts) {
		t.Error("TLS Secret Volume Mount not found in deployment")
	}

	volumes := deployment.Spec.Template.Spec.Volumes
	wantVol := v1.Volume{
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName: expectedSecretName,
			},
		},
		Name: "pachd-tls-cert",
	}

	if !ensureVolumePresent(wantVol, volumes) {
		t.Error("TLS Volume not found")
	}

}

func TestEnableconsoleTLSNoName(t *testing.T) {
	helmChartPath := "../pachyderm"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":        "LOCAL",
			"console.enabled":     "true",
			"ingress.tls.enabled": "true",
			"ingress.host":        "http://blah.com",
		},
	}

	_, err := helm.RenderTemplateE(t, options, helmChartPath, "secret", []string{"templates/console/ingress.yaml"})
	if err == nil {
		t.Error("Template should error")
	}
}

func TestEnableconsoleTLSExistingSecret(t *testing.T) {
	helmChartPath := "../pachyderm"
	expectedSecretName := "blah"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":           "LOCAL",
			"console.enabled":        "true",
			"ingress.tls.enabled":    "true",
			"ingress.tls.secretName": expectedSecretName,
			"ingress.enabled":        "true",
			"ingress.host":           "http://blah.com",
		},
	}

	output := helm.RenderTemplate(t, options, helmChartPath, "secret", []string{"templates/ingress/ingress.yaml"})
	var ingress *v1beta1.Ingress

	helm.UnmarshalK8SYaml(t, output, &ingress)
	found := false
	for _, t := range ingress.Spec.TLS {
		if t.SecretName == expectedSecretName {
			found = true
		}
	}
	if !found {
		t.Errorf("Expected TLS Secret not in ingress")
	}

}

func TestconsoleTLSNewSecretNoEnable(t *testing.T) {
	helmChartPath := "../pachyderm"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":              "LOCAL",
			"console.enabled":           "true",
			"ingress.tls.newSecret.crt": "blah",
			"ingress.host":              "http://blah.com",
		},
	}

	_, err := helm.RenderTemplateE(t, options, helmChartPath, "secret", []string{"templates/console/ingress.yaml"})
	if err == nil {
		t.Error("Template should error")
	}
}

func TestPachTLSNewSecretNoEnable(t *testing.T) {
	helmChartPath := "../pachyderm"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":            "LOCAL",
			"pachd.tls.newSecret.crt": "blah",
		},
	}

	_, err := helm.RenderTemplateE(t, options, helmChartPath, "deployment", []string{"templates/pachd/deployment.yaml"})
	if err == nil {
		t.Error("Template should error")
	}
}

func TestPachTLSNewSecret(t *testing.T) {
	helmChartPath := "../pachyderm"
	secretName := "newsecret1234"
	key := "blah"
	crt := "blah"
	options := &helm.Options{
		SetValues: map[string]string{
			"deployTarget":               "LOCAL",
			"pachd.tls.enabled":          "true",
			"pachd.tls.newSecret.create": "true",
			"pachd.tls.newSecret.crt":    crt,
			"pachd.tls.newSecret.key":    key,
			"pachd.tls.secretName":       secretName,
		},
	}
	manifests := []string{
		"templates/pachd/tls-secret.yaml",
		"templates/pachd/deployment.yaml",
	}

	renderedManifest := helm.RenderTemplate(t, options, helmChartPath, "deployment", manifests)
	objects, _ := manifestToObjects(renderedManifest)

	for _, obj := range objects {
		switch obj := obj.(type) {
		case *v1.Secret:
			if obj.Name != secretName {
				t.Error("Secret name must match passed in value")
			}
			gotCRT := obj.Data["tls.crt"]
			gotKey := obj.Data["tls.key"]

			if key != string(gotKey) {
				t.Error("Key does not match in secret")
			}
			if crt != string(gotCRT) {
				t.Error("Cert does not match in secret")
			}

		case *appsv1.Deployment:
			for _, c := range obj.Spec.Template.Spec.Containers {
				//Check volume mount pach-tls-cert exists and set correctly
				//Check volume pach-tls-cert and set to correct secret name
				if c.Name == "pachd" {
					want := v1.VolumeMount{
						Name:      "pachd-tls-cert",
						MountPath: "/pachd-tls-cert",
					}

					if !ensureVolumeMountPresent(want, c.VolumeMounts) {
						t.Error("TLS Secret Volume Mount not found in deployment")
					}

				}
			}
			volumes := obj.Spec.Template.Spec.Volumes
			wantVol := v1.Volume{
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: secretName,
					},
				},
				Name: "pachd-tls-cert",
			}

			if !ensureVolumePresent(wantVol, volumes) {
				t.Error("TLS Volume not found")
			}
		}
	}
}
