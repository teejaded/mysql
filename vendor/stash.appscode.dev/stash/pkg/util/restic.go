package util

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"kmodules.xyz/client-go/meta"
	api "stash.appscode.dev/stash/apis/stash/v1alpha1"
	stash_listers "stash.appscode.dev/stash/client/listers/stash/v1alpha1"
)

func GetAppliedRestic(m map[string]string) (*api.Restic, error) {
	data := GetString(m, api.LastAppliedConfiguration)
	if data == "" {
		return nil, nil
	}
	obj, err := meta.UnmarshalFromJSON([]byte(data), api.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}
	restic, ok := obj.(*api.Restic)
	if !ok {
		return nil, fmt.Errorf("%s annotations has invalid Rectic object", api.LastAppliedConfiguration)
	}
	return restic, nil
}

func FindRestic(lister stash_listers.ResticLister, obj metav1.ObjectMeta) (*api.Restic, error) {
	restics, err := lister.Restics(obj.Namespace).List(labels.Everything())
	if kerr.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	result := make([]*api.Restic, 0)
	for _, restic := range restics {
		selector, err := metav1.LabelSelectorAsSelector(&restic.Spec.Selector)
		if err != nil {
			return nil, err
		}
		if selector.Matches(labels.Set(obj.Labels)) && restic.DeletionTimestamp == nil {
			result = append(result, restic)
		}
	}
	if len(result) > 1 {
		var msg bytes.Buffer
		msg.WriteString(fmt.Sprintf("Workload %s/%s matches multiple Restics:", obj.Namespace, obj.Name))
		for i, restic := range result {
			if i > 0 {
				msg.WriteString(", ")
			}
			msg.WriteString(restic.Name)
		}
		return nil, errors.New(msg.String())
	} else if len(result) == 1 {
		return result[0], nil
	}
	return nil, nil
}

func ResticEqual(old, new *api.Restic) bool {
	var oldSpec, newSpec *api.ResticSpec
	if old != nil {
		oldSpec = &old.Spec
	}
	if new != nil {
		newSpec = &new.Spec
	}
	return reflect.DeepEqual(oldSpec, newSpec)
}
