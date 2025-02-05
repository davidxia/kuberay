package util

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LastCondition returns the most recent condition from a list of conditions.
func LastCondition(conditions []metav1.Condition) *metav1.Condition {
	if len(conditions) == 0 {
		return nil
	}

	var lastCondition *metav1.Condition
	for i := range conditions {
		if lastCondition == nil || conditions[i].LastTransitionTime.After(lastCondition.LastTransitionTime.Time) {
			lastCondition = &conditions[i]
		}
	}

	return lastCondition
}
