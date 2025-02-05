package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestLastCondition(t *testing.T) {
	tests := []struct {
		expectedCondition *metav1.Condition
		name              string
		conditions        []metav1.Condition
	}{
		{
			name:              "passing no Conditions should return nil pointer",
			conditions:        []metav1.Condition{},
			expectedCondition: nil,
		},
		{
			name: "passing multiple Conditions should return the most recent one",
			conditions: []metav1.Condition{
				{
					Type:    string(rayv1.RayClusterReplicaFailure),
					Status:  metav1.ConditionFalse,
					Reason:  rayv1.HeadPodNotFound,
					Message: "Head Pod not found",
					LastTransitionTime: metav1.Time{
						Time: time.Date(2024, 7, 21, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					Type:    string(rayv1.RayClusterProvisioned),
					Status:  metav1.ConditionTrue,
					Reason:  rayv1.AllPodRunningAndReadyFirstTime,
					Message: "All Ray Pods are ready for the first time",
					LastTransitionTime: metav1.Time{
						Time: time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					Type:    string(rayv1.HeadPodReady),
					Status:  metav1.ConditionTrue,
					Reason:  rayv1.HeadPodRunningAndReady,
					Message: "Head Pod ready",
					LastTransitionTime: metav1.Time{
						Time: time.Date(2024, 11, 5, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			expectedCondition: &metav1.Condition{
				Type:    string(rayv1.RayClusterProvisioned),
				Status:  metav1.ConditionTrue,
				Reason:  rayv1.AllPodRunningAndReadyFirstTime,
				Message: "All Ray Pods are ready for the first time",
				LastTransitionTime: metav1.Time{
					Time: time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedCondition, LastCondition(tc.conditions))
		})
	}
}
