package framework

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/appscode/go/crypto/rand"
	"github.com/appscode/go/log"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1 "kmodules.xyz/client-go/core/v1"
	meta_util "kmodules.xyz/client-go/meta"
	store "kmodules.xyz/objectstore-api/api/v1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha1"
	"kubedb.dev/mysql/pkg/controller"
	"stash.appscode.dev/stash/pkg/restic"
)

func (fi *Invocation) SecretForLocalBackend() *core.Secret {
	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix(fi.app + "-local"),
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{},
	}
}

func (fi *Invocation) SecretForS3Backend() *core.Secret {
	if os.Getenv(store.AWS_ACCESS_KEY_ID) == "" ||
		os.Getenv(store.AWS_SECRET_ACCESS_KEY) == "" {
		return &core.Secret{}
	}

	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix(fi.app + "-s3"),
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{
			store.AWS_ACCESS_KEY_ID:     []byte(os.Getenv(store.AWS_ACCESS_KEY_ID)),
			store.AWS_SECRET_ACCESS_KEY: []byte(os.Getenv(store.AWS_SECRET_ACCESS_KEY)),
		},
	}
}

func (fi *Invocation) SecretForGCSBackend() *core.Secret {
	if os.Getenv(store.GOOGLE_PROJECT_ID) == "" ||
		(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" && os.Getenv(store.GOOGLE_SERVICE_ACCOUNT_JSON_KEY) == "") {
		return &core.Secret{}
	}

	jsonKey := os.Getenv(store.GOOGLE_SERVICE_ACCOUNT_JSON_KEY)
	if jsonKey == "" {
		if keyBytes, err := ioutil.ReadFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); err == nil {
			jsonKey = string(keyBytes)
		}
	}

	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix(fi.app + "-gcs"),
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{
			store.GOOGLE_PROJECT_ID:               []byte(os.Getenv(store.GOOGLE_PROJECT_ID)),
			store.GOOGLE_SERVICE_ACCOUNT_JSON_KEY: []byte(jsonKey),
		},
	}
}

func (fi *Invocation) SecretForAzureBackend() *core.Secret {
	if os.Getenv(store.AZURE_ACCOUNT_NAME) == "" ||
		os.Getenv(store.AZURE_ACCOUNT_KEY) == "" {
		return &core.Secret{}
	}

	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix(fi.app + "-azure"),
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{
			store.AZURE_ACCOUNT_NAME: []byte(os.Getenv(store.AZURE_ACCOUNT_NAME)),
			store.AZURE_ACCOUNT_KEY:  []byte(os.Getenv(store.AZURE_ACCOUNT_KEY)),
		},
	}
}

func (fi *Invocation) SecretForSwiftBackend() *core.Secret {
	if os.Getenv(store.OS_AUTH_URL) == "" ||
		(os.Getenv(store.OS_TENANT_ID) == "" && os.Getenv(store.OS_TENANT_NAME) == "") ||
		os.Getenv(store.OS_USERNAME) == "" ||
		os.Getenv(store.OS_PASSWORD) == "" {
		return &core.Secret{}
	}

	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix(fi.app + "-swift"),
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{
			store.OS_AUTH_URL:    []byte(os.Getenv(store.OS_AUTH_URL)),
			store.OS_TENANT_ID:   []byte(os.Getenv(store.OS_TENANT_ID)),
			store.OS_TENANT_NAME: []byte(os.Getenv(store.OS_TENANT_NAME)),
			store.OS_USERNAME:    []byte(os.Getenv(store.OS_USERNAME)),
			store.OS_PASSWORD:    []byte(os.Getenv(store.OS_PASSWORD)),
			store.OS_REGION_NAME: []byte(os.Getenv(store.OS_REGION_NAME)),
		},
	}
}

func (i *Invocation) PatchSecretForRestic(secret *core.Secret) *core.Secret {
	if secret == nil {
		return secret
	}

	secret.StringData = v1.UpsertMap(secret.StringData, map[string]string{
		restic.RESTIC_PASSWORD: "RESTIC_PASSWORD",
	})

	return secret
}

// TODO: Add more methods for Swift, Backblaze B2, Rest server backend.
func (f *Framework) CreateSecret(obj *core.Secret) error {
	_, err := f.kubeClient.CoreV1().Secrets(obj.Namespace).Create(obj)
	return err
}

func (f *Framework) UpdateSecret(meta metav1.ObjectMeta, transformer func(core.Secret) core.Secret) error {
	attempt := 0
	for ; attempt < maxAttempts; attempt = attempt + 1 {
		cur, err := f.kubeClient.CoreV1().Secrets(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
		if kerr.IsNotFound(err) {
			return nil
		} else if err == nil {
			modified := transformer(*cur)
			_, err = f.kubeClient.CoreV1().Secrets(cur.Namespace).Update(&modified)
			if err == nil {
				return nil
			}
		}
		log.Errorf("Attempt %d failed to update Secret %s@%s due to %s.", attempt, cur.Name, cur.Namespace, err)
		time.Sleep(updateRetryInterval)
	}
	return fmt.Errorf("failed to update Secret %s@%s after %d attempts", meta.Name, meta.Namespace, attempt)
}

func (f *Framework) GetMySQLRootPassword(mysql *api.MySQL) (string, error) {
	secret, err := f.kubeClient.CoreV1().Secrets(mysql.Namespace).Get(mysql.Spec.DatabaseSecret.SecretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	password := string(secret.Data[controller.KeyMySQLPassword])
	return password, nil
}

func (f *Framework) GetSecretCred(secretMeta metav1.ObjectMeta, key string) (string, error) {
	secret, err := f.kubeClient.CoreV1().Secrets(secretMeta.Namespace).Get(secretMeta.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	data := string(secret.Data[key])
	return data, nil
}

func (f *Framework) GetSecret(meta metav1.ObjectMeta) (*core.Secret, error) {
	return f.kubeClient.CoreV1().Secrets(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
}

func (f *Framework) DeleteSecret(meta metav1.ObjectMeta) error {
	return f.kubeClient.CoreV1().Secrets(meta.Namespace).Delete(meta.Name, deleteInForeground())
}

func (f *Framework) EventuallyDBSecretCount(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	labelMap := map[string]string{
		api.LabelDatabaseKind: api.ResourceKindMySQL,
		api.LabelDatabaseName: meta.Name,
	}
	labelSelector := labels.SelectorFromSet(labelMap)

	return Eventually(
		func() int {
			secretList, err := f.kubeClient.CoreV1().Secrets(meta.Namespace).List(
				metav1.ListOptions{
					LabelSelector: labelSelector.String(),
				},
			)
			Expect(err).NotTo(HaveOccurred())

			return len(secretList.Items)
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) CheckSecret(secret *core.Secret) error {
	_, err := f.kubeClient.CoreV1().Secrets(f.namespace).Get(secret.Name, metav1.GetOptions{})
	return err
}

func (i *Invocation) SecretForDatabaseAuthentication(meta metav1.ObjectMeta, mangedByKubeDB bool) *core.Secret {
	//mangedByKubeDB mimics a secret created and manged by kubedb and not user.
	// It should get deleted during wipeout
	randPassword := ""

	// if the password starts with "-" it will cause error in bash scripts (in mongodb-tools)
	for randPassword = rand.GeneratePassword(); randPassword[0] == '-'; {
	}
	var dbObjectMeta = metav1.ObjectMeta{
		Name:      fmt.Sprintf("kubedb-%v-%v", meta.Name, CustomSecretSuffix),
		Namespace: meta.Namespace,
	}
	if mangedByKubeDB {
		dbObjectMeta.Labels = map[string]string{
			meta_util.ManagedByLabelKey: api.GenericKey,
		}
	}
	return &core.Secret{
		ObjectMeta: dbObjectMeta,
		Type:       core.SecretTypeOpaque,
		StringData: map[string]string{
			KeyMySQLUser:     mysqlUser,
			KeyMySQLPassword: randPassword,
		},
	}
}
