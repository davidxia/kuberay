package scale

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

func TestRayScaleClusterComplete(t *testing.T) {
	cmd := &cobra.Command{Use: "scale"}

	tests := []struct {
		name                string
		namespace           string
		expectedNamespace   string
		expectedWorkerGroup string
		expectedError       string
		args                []string
		hasErr              bool
	}{
		{
			name:   "should error if no worker group is set",
			args:   []string{},
			hasErr: true,
		},
		{
			name:   "should error if there are too many arguments",
			args:   []string{"my-workergroup", "extra-arg"},
			hasErr: true,
		},
		{
			name:                "should succeed with default values if arguments are valid",
			args:                []string{"my-workergroup"},
			expectedNamespace:   "default",
			expectedWorkerGroup: "my-workergroup",
			hasErr:              false,
		},
		{
			name:                "should succeed with specified values if arguments are valid",
			args:                []string{"my-workergroup"},
			namespace:           "DEADBEEF",
			expectedNamespace:   "DEADBEEF",
			expectedWorkerGroup: "my-workergroup",
			hasErr:              false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
			fakeScaleClusterOptions := NewScaleClusterOptions(testStreams)
			if tc.namespace != "" {
				fakeScaleClusterOptions.configFlags.Namespace = &tc.namespace
			}

			err := fakeScaleClusterOptions.Complete(cmd, tc.args)

			if tc.hasErr {
				assert.Error(t, err)
				if tc.expectedError != "" {
					assert.EqualError(t, err, tc.expectedError)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNamespace, *fakeScaleClusterOptions.configFlags.Namespace)
				assert.Equal(t, tc.expectedWorkerGroup, fakeScaleClusterOptions.workerGroup)
			}
		})
	}
}

func TestRayScaleClusterValidate(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	replicas := int32(4)

	kubeConfigWithCurrentContext, err := util.CreateTempKubeConfigFile(t, "test-context")
	assert.NoError(t, err)

	kubeConfigWithoutCurrentContext, err := util.CreateTempKubeConfigFile(t, "")
	assert.NoError(t, err)

	tests := []struct {
		name        string
		opts        *ScaleClusterOptions
		expect      string
		expectError string
	}{
		{
			name: "should error when no context is set",
			opts: &ScaleClusterOptions{
				configFlags: &genericclioptions.ConfigFlags{
					KubeConfig: &kubeConfigWithoutCurrentContext,
				},
				ioStreams: &testStreams,
			},
			expectError: "no context is currently set, use \"--context\" or \"kubectl config use-context <context>\" to select a new one",
		},
		{
			name: "should error when no RayCluster is set",
			opts: &ScaleClusterOptions{
				configFlags: &genericclioptions.ConfigFlags{
					KubeConfig: &kubeConfigWithCurrentContext,
				},
				ioStreams: &testStreams,
			},
			expectError: "must specify -c/--raycluster",
		},
		{
			name: "should error when no replicas are set",
			opts: &ScaleClusterOptions{
				configFlags: &genericclioptions.ConfigFlags{
					KubeConfig: &kubeConfigWithCurrentContext,
				},
				ioStreams: &testStreams,
				cluster:   "test-cluster",
			},
			expectError: "must specify -r/--replicas with a non-negative integer",
		},
		{
			name: "successful validation call",
			opts: &ScaleClusterOptions{
				configFlags: &genericclioptions.ConfigFlags{
					KubeConfig: &kubeConfigWithCurrentContext,
				},
				ioStreams: &testStreams,
				cluster:   "test-cluster",
				replicas:  &replicas,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.expectError != "" {
				assert.EqualError(t, err, tc.expectError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRayScaleClusterRun(t *testing.T) {
	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	testNamespace, testContext, workerGroup, cluster := "test-namespace", "test-context", "worker-group-1", "my-cluster"
	replicas, desiredReplicas := int32(1), int32(3)

	tests := []struct {
		name           string
		expectedOutput string
		expectedError  string
		rayClusters    []runtime.Object
	}{
		{
			name:          "should error when cluster doesn't exist",
			rayClusters:   []runtime.Object{},
			expectedError: "failed to scale worker group",
		},
		{
			name: "should error when worker group doesn't exist",
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cluster,
						Namespace: testNamespace,
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
					},
				},
			},
			expectedError: fmt.Sprintf("worker group %s not found", workerGroup),
		},
		{
			name: "should not do anything when the desired replicas is the same as the current replicas",
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cluster,
						Namespace: testNamespace,
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: workerGroup,
								Replicas:  &desiredReplicas,
							},
						},
					},
				},
			},
			expectedOutput: fmt.Sprintf("already has %d replicas", desiredReplicas),
		},
		{
			name: "should do something when ...",
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cluster,
						Namespace: testNamespace,
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: workerGroup,
								Replicas:  &replicas,
							},
						},
					},
				},
			},
			expectedOutput: fmt.Sprintf("Scaled worker group %s", workerGroup),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeScaleClusterOptions := ScaleClusterOptions{
				configFlags: &genericclioptions.ConfigFlags{
					Context:   &testContext,
					Namespace: &testNamespace,
				},
				ioStreams:   &testStreams,
				replicas:    &desiredReplicas,
				workerGroup: workerGroup,
				cluster:     cluster,
			}

			kubeClientSet := kubefake.NewClientset()
			rayClient := rayClientFake.NewSimpleClientset(tc.rayClusters...)
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			var buf bytes.Buffer
			err := fakeScaleClusterOptions.Run(context.Background(), k8sClients, &buf)
			if tc.expectedError == "" {
				assert.NoError(t, err)
				assert.Contains(t, buf.String(), tc.expectedOutput)
			} else {
				assert.ErrorContains(t, err, tc.expectedError)
			}

			// if e, a := resbuffer.String(), resBuf.String(); e != a {
			// 	t.Errorf("\nexpected\n%v\ngot\n%v", e, a)
			// }
		})
	}
}
