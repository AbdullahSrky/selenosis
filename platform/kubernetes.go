package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/alcounit/selenosis/tools"
	"github.com/sirupsen/logrus"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

var (
	label        = "selenosis.app.type"
	quotaName    = "selenosis-pod-limit"
	browserPorts = struct {
		selenium, vnc intstr.IntOrString
	}{
		selenium: intstr.FromString("4444"),
		vnc:      intstr.FromString("5900"),
	}

	defaultsAnnotations = struct {
		testName, browserName, browserVersion, screenResolution, enableVNC, timeZone string
	}{
		testName:         "testName",
		browserName:      "browserName",
		browserVersion:   "browserVersion",
		screenResolution: "SCREEN_RESOLUTION",
		enableVNC:        "ENABLE_VNC",
		timeZone:         "TZ",
	}
	defaultLabels = struct {
		serviceType, appType, session string
	}{
		serviceType: "type",
		appType:     label,
		session:     "session",
	}
)

// ClientConfig ...
type ClientConfig struct {
	Namespace           string
	Service             string
	ServicePort         string
	ImagePullSecretName string
	ProxyImage          string
	ReadinessTimeout    time.Duration
	IdleTimeout         time.Duration
}

// Client ...
type Client struct {
	ns        string
	svc       string
	svcPort   intstr.IntOrString
	clientset kubernetes.Interface
	service   ServiceInterface
	quota     QuotaInterface
}

// NewClient ...
func NewClient(c ClientConfig) (Platform, error) {

	conf, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to build client: %v", err)
	}

	service := &service{
		ns:                  c.Namespace,
		clientset:           clientset,
		svc:                 c.Service,
		svcPort:             intstr.FromString(c.ServicePort),
		imagePullSecretName: c.ImagePullSecretName,
		proxyImage:          c.ProxyImage,
		readinessTimeout:    c.ReadinessTimeout,
		idleTimeout:         c.IdleTimeout,
	}

	quota := &quota{
		ns:        c.Namespace,
		clientset: clientset,
	}

	return &Client{
		ns:        c.Namespace,
		clientset: clientset,
		svc:       c.Service,
		svcPort:   intstr.FromString(c.ServicePort),
		service:   service,
		quota:     quota,
	}, nil

}

func (cl *Client) Service() ServiceInterface {
	return cl.service
}

func (cl *Client) Quota() QuotaInterface {
	return cl.quota
}

// List ...
func (cl *Client) State() (PlatformState, error) {
	context := context.Background()
	pods, err := cl.clientset.CoreV1().Pods(cl.ns).List(context, metav1.ListOptions{})

	if err != nil {
		return PlatformState{}, fmt.Errorf("failed to get pods: %v", err)
	}

	var services []Service
	var workers []Worker

	for _, pod := range pods.Items {
		podName := pod.GetName()
		creationTime := pod.CreationTimestamp.Time

		var status ServiceStatus
		switch pod.Status.Phase {
		case apiv1.PodRunning:
			status = Running
		case apiv1.PodPending:
			status = Pending
		default:
			status = Unknown
		}

		if application, ok := pod.GetLabels()[label]; ok {
			switch application {
			case "worker":
				workers = append(workers, Worker{
					Name:    podName,
					Labels:  pod.Labels,
					Status:  status,
					Started: creationTime,
				})

			case "browser":
				services = append(services, Service{
					SessionID: podName,
					URL: &url.URL{
						Scheme: "http",
						Host:   tools.BuildHostPort(podName, cl.svc, cl.svcPort.StrVal),
					},
					Labels: getRequestedCapabilities(pod.GetAnnotations()),
					CancelFunc: func() {
						deletePod(cl.clientset, cl.ns, podName)
					},
					Status:  status,
					Started: creationTime,
				})
			}
		}
	}

	return PlatformState{
		Services: services,
		Workers:  workers,
	}, nil

}

// Watch ...
func (cl *Client) Watch() <-chan Event {
	ch := make(chan Event)
	namespace := informers.WithNamespace(cl.ns)
	labels := informers.WithTweakListOptions(func(list *metav1.ListOptions) {
		list.LabelSelector = label
	})

	sharedIformer := informers.NewSharedInformerFactoryWithOptions(cl.clientset, 30*time.Second, namespace, labels)

	podEventFunc := func(obj interface{}, eventType EventType) {
		if pod, ok := obj.(*apiv1.Pod); ok {
			if application, ok := pod.GetLabels()[label]; ok {
				podName := pod.GetName()
				creationTime := pod.CreationTimestamp.Time

				var status ServiceStatus
				switch pod.Status.Phase {
				case apiv1.PodRunning:
					status = Running
				case apiv1.PodPending:
					status = Pending
				default:
					status = Unknown
				}

				switch application {
				case "worker":
					ch <- Event{
						Type: eventType,
						PlatformObject: Worker{
							Name:    podName,
							Labels:  pod.Labels,
							Status:  status,
							Started: creationTime,
						},
					}

				case "browser":
					ch <- Event{
						Type: eventType,
						PlatformObject: Service{
							SessionID: podName,
							URL: &url.URL{
								Scheme: "http",
								Host:   tools.BuildHostPort(podName, cl.svc, cl.svcPort.StrVal),
							},
							Labels: getRequestedCapabilities(pod.GetAnnotations()),
							CancelFunc: func() {
								deletePod(cl.clientset, cl.ns, podName)
							},
							Status:  status,
							Started: creationTime,
						},
					}
				}
			}
		}
	}
	sharedIformer.Core().V1().Pods().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				podEventFunc(obj, Added)
			},
			UpdateFunc: func(old interface{}, new interface{}) {
				podEventFunc(new, Updated)
			},
			DeleteFunc: func(obj interface{}) {
				podEventFunc(obj, Deleted)
			},
		},
	)

	quotaEventFunc := func(obj interface{}, eventType EventType) {
		if rq, ok := obj.(*apiv1.ResourceQuota); ok {
			if _, ok := rq.GetLabels()[label]; ok {
				rqName := rq.GetName()
				ch <- Event{
					Type: eventType,
					PlatformObject: Quota{
						Name:            rqName,
						CurrentMaxLimit: rq.Spec.Hard.Pods().Value(),
					},
				}
			}
		}
	}
	sharedIformer.Core().V1().ResourceQuotas().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				quotaEventFunc(obj, Added)
			},
			UpdateFunc: func(old interface{}, new interface{}) {
				quotaEventFunc(new, Updated)
			},
			DeleteFunc: func(obj interface{}) {
				quotaEventFunc(obj, Deleted)
			},
		},
	)

	var neverStop <-chan struct{} = make(chan struct{})
	sharedIformer.Start(neverStop)
	return ch
}

type service struct {
	ns                  string
	svc                 string
	svcPort             intstr.IntOrString
	imagePullSecretName string
	proxyImage          string
	readinessTimeout    time.Duration
	idleTimeout         time.Duration
	clientset           kubernetes.Interface
}

// Create ...
func (cl *service) Create(layout ServiceSpec) (Service, error) {
	annontations := map[string]string{
		defaultsAnnotations.browserName:    layout.Template.BrowserName,
		defaultsAnnotations.browserVersion: layout.Template.BrowserVersion,
		defaultsAnnotations.testName:       layout.RequestedCapabilities.TestName,
	}

	labels := map[string]string{
		defaultLabels.serviceType: "browser",
		defaultLabels.appType:     "browser",
		defaultLabels.session:     layout.SessionID,
	}

	envVar := func(name string) (i int, b bool) {
		for i, slice := range layout.Template.Spec.EnvVars {
			if slice.Name == name {
				return i, true
			}
		}
		return -1, false
	}

	i, b := envVar(defaultsAnnotations.screenResolution)
	if layout.RequestedCapabilities.ScreenResolution != "" {
		if !b {
			layout.Template.Spec.EnvVars = append(layout.Template.Spec.EnvVars,
				apiv1.EnvVar{Name: defaultsAnnotations.screenResolution,
					Value: layout.RequestedCapabilities.ScreenResolution})
		} else {
			layout.Template.Spec.EnvVars[i] = apiv1.EnvVar{Name: defaultsAnnotations.screenResolution, Value: layout.RequestedCapabilities.ScreenResolution}
		}
		annontations[defaultsAnnotations.screenResolution] = layout.RequestedCapabilities.ScreenResolution
	} else {
		if b {
			annontations[defaultsAnnotations.screenResolution] = layout.Template.Spec.EnvVars[i].Value
		}
	}

	i, b = envVar(defaultsAnnotations.enableVNC)
	if layout.RequestedCapabilities.VNC {
		vnc := fmt.Sprintf("%v", layout.RequestedCapabilities.VNC)
		if !b {
			layout.Template.Spec.EnvVars = append(layout.Template.Spec.EnvVars, apiv1.EnvVar{Name: defaultsAnnotations.enableVNC, Value: vnc})
		} else {
			layout.Template.Spec.EnvVars[i] = apiv1.EnvVar{Name: defaultsAnnotations.enableVNC, Value: vnc}
		}
		annontations[defaultsAnnotations.enableVNC] = vnc
	} else {
		if b {
			annontations[defaultsAnnotations.enableVNC] = layout.Template.Spec.EnvVars[i].Value
		}
	}

	i, b = envVar(defaultsAnnotations.timeZone)
	if layout.RequestedCapabilities.TimeZone != "" {
		if !b {
			layout.Template.Spec.EnvVars = append(layout.Template.Spec.EnvVars, apiv1.EnvVar{Name: defaultsAnnotations.timeZone, Value: layout.RequestedCapabilities.TimeZone})
		} else {
			layout.Template.Spec.EnvVars[i] = apiv1.EnvVar{Name: defaultsAnnotations.timeZone, Value: layout.RequestedCapabilities.TimeZone}
		}
		annontations[defaultsAnnotations.timeZone] = layout.RequestedCapabilities.TimeZone
	} else {
		if b {
			annontations[defaultsAnnotations.timeZone] = layout.Template.Spec.EnvVars[i].Value
		}
	}

	if layout.Template.Meta.Labels == nil {
		layout.Template.Meta.Labels = make(map[string]string)
	}

	for k, v := range labels {
		layout.Template.Meta.Labels[k] = v
	}

	if layout.Template.Meta.Annotations == nil {
		layout.Template.Meta.Annotations = make(map[string]string)
	}

	if caps, err := json.Marshal(annontations); err == nil {
		layout.Template.Meta.Annotations["capabilities"] = string(caps)
	}

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        layout.SessionID,
			Labels:      layout.Template.Meta.Labels,
			Annotations: layout.Template.Meta.Annotations,
		},
		Spec: apiv1.PodSpec{
			Hostname:  layout.SessionID,
			Subdomain: cl.svc,
			Containers: []apiv1.Container{
				{
					Name:  "browser",
					Image: layout.Template.Image,
					SecurityContext: &apiv1.SecurityContext{
						Privileged:   layout.Template.Privileged,
						Capabilities: getCapabilities(layout.Template.Capabilities),
					},
					Env:             layout.Template.Spec.EnvVars,
					Ports:           getBrowserPorts(),
					Resources:       layout.Template.Spec.Resources,
					VolumeMounts:    getVolumeMounts(layout.Template.Spec.VolumeMounts),
					ImagePullPolicy: apiv1.PullIfNotPresent,
				},
				{
					Name:  "seleniferous",
					Image: cl.proxyImage,
					Ports: getSidecarPorts(cl.svcPort),
					Command: []string{
						"/seleniferous", "--listhen-port", cl.svcPort.StrVal, "--proxy-default-path", path.Join(layout.Template.Path, "session"), "--idle-timeout", cl.idleTimeout.String(), "--namespace", cl.ns,
					},
					ImagePullPolicy: apiv1.PullIfNotPresent,
				},
			},
			Volumes:          getVolumes(layout.Template.Volumes),
			NodeSelector:     layout.Template.Spec.NodeSelector,
			HostAliases:      layout.Template.Spec.HostAliases,
			RestartPolicy:    apiv1.RestartPolicyNever,
			Affinity:         &layout.Template.Spec.Affinity,
			DNSConfig:        &layout.Template.Spec.DNSConfig,
			Tolerations:      layout.Template.Spec.Tolerations,
			ImagePullSecrets: getImagePullSecretList(cl.imagePullSecretName),
			SecurityContext:  getSecurityContext(layout.Template.RunAs),
		},
	}

	logger := logrus.WithFields(logrus.Fields{
		"function":   "Create",
		"session_id": layout.SessionID,
		"image":      layout.Template.Image,
		"namespace":  cl.ns,
	})

	logger.Info("Creating browser pod")
	startTime := time.Now()

	context := context.Background()
	pod, err := cl.clientset.CoreV1().Pods(cl.ns).Create(context, pod, metav1.CreateOptions{})

	if err != nil {
		logger.WithError(err).Error("Failed to create pod")
		return Service{}, fmt.Errorf("failed to create pod %v", err)
	}

	podName := pod.GetName()
	logger = logger.WithField("pod_name", podName)
	logger.WithField("creation_time", time.Since(startTime)).Info("Pod created successfully")

	cancel := func() {
		cl.Delete(podName)
	}

	watchTimeout := ptr.To(int64(cl.readinessTimeout.Seconds()))
	logger.WithField("watch_timeout", cl.readinessTimeout).Info("Starting pod status watch")

	w, err := cl.clientset.CoreV1().Pods(cl.ns).Watch(context, metav1.ListOptions{
		FieldSelector:  fields.OneTermEqualSelector("metadata.name", podName).String(),
		TimeoutSeconds: watchTimeout,
	})

	if err != nil {
		logger.WithError(err).Error("Failed to watch pod status")
		return Service{}, fmt.Errorf("failed to watch pod status: %v", err)
	}

	statusFn := func() error {
		defer w.Stop()
		var watchedPod *apiv1.Pod
		watchStart := time.Now()

		for event := range w.ResultChan() {
			elapsed := time.Since(watchStart)

			switch event.Type {
			case watch.Error:
				logger.WithFields(logrus.Fields{
					"elapsed": elapsed,
					"event":   event.Object.GetObjectKind().GroupVersionKind().String(),
				}).Error("Received error event while watching pod")
				return fmt.Errorf("received error while watching pod: %s",
					event.Object.GetObjectKind().GroupVersionKind().String())
			case watch.Deleted, watch.Added, watch.Modified:
				watchedPod = event.Object.(*apiv1.Pod)
				logger.WithFields(logrus.Fields{
					"elapsed":    elapsed,
					"event_type": event.Type,
					"phase":      watchedPod.Status.Phase,
				}).Debug("Pod status event")
			default:
				logger.WithFields(logrus.Fields{
					"elapsed":    elapsed,
					"event_type": event.Type,
				}).Warn("Unknown event type")
				return fmt.Errorf("received unknown event type %s while watching pod", event.Type)
			}

			if event.Type == watch.Deleted {
				logger.WithField("elapsed", elapsed).Error("Pod was deleted before becoming available")
				return errors.New("pod was deleted before becoming available")
			}

			switch watchedPod.Status.Phase {
			case apiv1.PodPending:
				logger.WithFields(logrus.Fields{
					"elapsed":          elapsed,
					"container_status": getPodContainerStatus(watchedPod),
				}).Debug("Pod is pending")
				continue
			case apiv1.PodSucceeded, apiv1.PodFailed:
				reason := getPodFailureReason(watchedPod)
				logger.WithFields(logrus.Fields{
					"elapsed": elapsed,
					"phase":   watchedPod.Status.Phase,
					"reason":  reason,
				}).Error("Pod exited early")
				return fmt.Errorf("pod exited early with status %s: %s", watchedPod.Status.Phase, reason)
			case apiv1.PodRunning:
				logger.WithFields(logrus.Fields{
					"elapsed":    elapsed,
					"total_time": time.Since(startTime),
				}).Info("Pod is running")
				return nil
			case apiv1.PodUnknown:
				logger.WithField("elapsed", elapsed).Error("Pod state is unknown")
				return errors.New("couldn't obtain pod state")
			default:
				logger.WithFields(logrus.Fields{
					"elapsed": elapsed,
					"phase":   watchedPod.Status.Phase,
				}).Error("Pod has unknown status")
				return errors.New("pod has unknown status")
			}
		}
		logger.WithField("elapsed", time.Since(watchStart)).Error("Pod watch channel closed without pod running")
		return fmt.Errorf("pod wasn't running after %v", time.Since(watchStart))
	}

	err = statusFn()
	if err != nil {
		logger.WithError(err).Error("Pod status check failed")
		cancel()
		return Service{}, fmt.Errorf("pod is not ready after creation: %v", err)
	}

	// Wait a bit for Chrome to initialize after pod becomes Running
	// This prevents "connection refused" errors when connecting too early
	initialWait := 3 * time.Second
	logger.WithField("initial_wait", initialWait).Info("Waiting for Chrome to initialize after pod start")
	time.Sleep(initialWait)

	// Get pod again to fetch its IP address (avoids DNS lookup issues)
	pod, err = cl.clientset.CoreV1().Pods(cl.ns).Get(context, podName, metav1.GetOptions{})
	if err != nil {
		logger.WithError(err).Error("Failed to get pod IP")
		cancel()
		return Service{}, fmt.Errorf("failed to get pod IP: %v", err)
	}

	podIP := pod.Status.PodIP
	if podIP == "" {
		logger.Error("Pod IP is empty")
		cancel()
		return Service{}, fmt.Errorf("pod IP is empty")
	}

	logger.WithField("pod_ip", podIP).Info("Using pod IP for direct connection (bypassing DNS)")

	// Use Pod IP directly instead of DNS name to avoid "no such host" errors
	u := &url.URL{
		Scheme: "http",
		Host:   podIP + ":" + browserPorts.selenium.StrVal,
	}

	logger.WithFields(logrus.Fields{
		"service_url":       u.String(),
		"readiness_timeout": cl.readinessTimeout,
		"using_pod_ip":      true,
	}).Info("Starting service readiness check")

	if err := waitForService(*u, cl.readinessTimeout); err != nil {
		logger.WithError(err).Error("Service readiness check failed")
		cancel()
		return Service{}, fmt.Errorf("container service is not ready %v: %v", u.String(), err)
	}

	// CRITICAL FIX: Keep using Pod IP instead of changing back to DNS name
	// This eliminates DNS lookup delays and "no such host" errors
	// u.Host = podName + "." + cl.svc + ":" + cl.svcPort.StrVal  // OLD: Changed to DNS
	// u already has Pod IP from line 581, keep it!

	logger.WithFields(logrus.Fields{
		"session_id":  podName,
		"service_url": u.String(),
		"pod_ip":      podIP,
	}).Info("Returning service with Pod IP (bypassing DNS)")

	return Service{
		SessionID: podName,
		URL:       u, // Keep Pod IP!
		Labels:    getRequestedCapabilities(pod.GetAnnotations()),
		CancelFunc: func() {
			cancel()
		},
		Status:  Running,
		Started: pod.CreationTimestamp.Time,
	}, nil
}

// Delete ...
func (cl *service) Delete(name string) error {
	return deletePod(cl.clientset, cl.ns, name)
}

// Logs ...
func (cl *service) Logs(ctx context.Context, name string) (io.ReadCloser, error) {
	req := cl.clientset.CoreV1().Pods(cl.ns).GetLogs(name, &apiv1.PodLogOptions{
		Container:  "browser",
		Follow:     true,
		Previous:   false,
		Timestamps: false,
	})
	return req.Stream(ctx)
}

type quota struct {
	ns        string
	clientset kubernetes.Interface
}

// Create ...
func (cl quota) Create(limit int64) (Quota, error) {
	context := context.Background()
	quantity, err := resource.ParseQuantity(strconv.FormatInt(limit, 10))
	if err != nil {
		return Quota{}, fmt.Errorf("failed to parse limit amount")
	}
	quota := &apiv1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:   quotaName,
			Labels: map[string]string{label: "quota"},
		},
		Spec: apiv1.ResourceQuotaSpec{
			Hard: map[apiv1.ResourceName]resource.Quantity{apiv1.ResourcePods: quantity},
		},
	}
	quota, err = cl.clientset.CoreV1().ResourceQuotas(cl.ns).Create(context, quota, metav1.CreateOptions{})
	if err != nil {
		return Quota{}, fmt.Errorf("failed to create resourceQuota")
	}
	return Quota{
		Name:            quota.GetName(),
		CurrentMaxLimit: quota.Spec.Hard.Pods().Value(),
	}, nil
}

func (cl quota) Get() (Quota, error) {
	context := context.Background()
	quota, err := cl.clientset.CoreV1().ResourceQuotas(cl.ns).Get(context, quotaName, metav1.GetOptions{})
	if err != nil {
		return Quota{}, fmt.Errorf("quota not found")
	}

	return Quota{
		Name:            quota.GetName(),
		CurrentMaxLimit: quota.Spec.Hard.Pods().Value(),
	}, nil
}

// Update ...
func (cl quota) Update(limit int64) (Quota, error) {
	context := context.Background()
	quantity, err := resource.ParseQuantity(strconv.FormatInt(limit, 10))
	if err != nil {
		return Quota{}, fmt.Errorf("failed to parse limit amount")
	}

	// CRITICAL FIX: Retry loop with exponential backoff for ResourceQuota conflicts
	// When multiple Selenosis replicas (30) update quota simultaneously, optimistic locking
	// causes conflicts. We retry with increasing delays to resolve this.
	// With 30 replicas, we need more retries to ensure all updates succeed.
	maxRetries := 10
	var quota *apiv1.ResourceQuota
	
	for i := 0; i < maxRetries; i++ {
		// Get current ResourceQuota to preserve ResourceVersion
		currentQuota, err := cl.clientset.CoreV1().ResourceQuotas(cl.ns).Get(context, quotaName, metav1.GetOptions{})
		if err != nil {
			return Quota{}, fmt.Errorf("failed to get current quota: %v", err)
		}

		// Update the spec with new limit, but keep existing ResourceVersion
		currentQuota.Spec.Hard[apiv1.ResourcePods] = quantity

		quota, err = cl.clientset.CoreV1().ResourceQuotas(cl.ns).Update(context, currentQuota, metav1.UpdateOptions{})
		if err == nil {
			// Success!
			return Quota{
				Name:            quota.GetName(),
				CurrentMaxLimit: quota.Spec.Hard.Pods().Value(),
			}, nil
		}

		// Check if it's a conflict error
		if strings.Contains(err.Error(), "object has been modified") {
			// Exponential backoff with jitter to reduce thundering herd
			// 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s, 51.2s
			baseBackoff := time.Duration(100*(1<<uint(i))) * time.Millisecond
			// Add jitter: ±25% randomness
			jitterPercent := float64(time.Now().UnixNano()%100) / 100.0 // 0.0 to 1.0
			jitter := time.Duration(float64(baseBackoff) * 0.25 * (2*jitterPercent - 1))
			backoff := baseBackoff + jitter
			
			// Cap at 5 seconds to avoid too long waits
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			
			time.Sleep(backoff)
			continue
		}

		// If it's not a conflict error, fail immediately
		return Quota{}, fmt.Errorf("resourse quota update error: %v", err)
	}

	// All retries exhausted - this is logged as warning, not critical error
	// Pod creation still succeeds, just quota tracking may be slightly off
	return Quota{}, fmt.Errorf("resourse quota update error: max retries (%d) exceeded", maxRetries)
}

func deletePod(clientset kubernetes.Interface, namespace, name string) error {
	context := context.Background()

	return clientset.CoreV1().Pods(namespace).Delete(context, name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To(int64(15)),
	})
}

func getBrowserPorts() []apiv1.ContainerPort {
	port := []apiv1.ContainerPort{}
	fn := func(name string, value int) {
		port = append(port, apiv1.ContainerPort{Name: name, ContainerPort: int32(value)})
	}

	fn("vnc", browserPorts.vnc.IntValue())
	fn("selenium", browserPorts.selenium.IntValue())

	return port
}

func getSidecarPorts(p intstr.IntOrString) []apiv1.ContainerPort {
	port := []apiv1.ContainerPort{}
	fn := func(name string, value int) {
		port = append(port, apiv1.ContainerPort{Name: name, ContainerPort: int32(value)})
	}
	fn("selenium", p.IntValue())
	return port
}

func getImagePullSecretList(secret string) []apiv1.LocalObjectReference {
	refList := make([]apiv1.LocalObjectReference, 0)
	if secret != "" {
		ref := apiv1.LocalObjectReference{
			Name: secret,
		}
		refList = append(refList, ref)
	}
	return refList
}

func getRequestedCapabilities(annotations map[string]string) map[string]string {
	if k, ok := annotations["capabilities"]; ok {
		capabilities := make(map[string]string)
		err := json.Unmarshal([]byte(k), &capabilities)
		if err == nil {
			return capabilities
		}
	}
	return nil
}

func getVolumeMounts(mounts []apiv1.VolumeMount) []apiv1.VolumeMount {
	vm := []apiv1.VolumeMount{
		{
			Name:      "dshm",
			MountPath: "/dev/shm",
		},
	}
	if mounts != nil {
		vm = append(vm, mounts...)
	}
	return vm
}

func getVolumes(volumes []apiv1.Volume) []apiv1.Volume {
	v := []apiv1.Volume{
		{
			Name: "dshm",
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{
					Medium: apiv1.StorageMediumMemory,
				},
			},
		},
	}
	if volumes != nil {
		v = append(v, volumes...)
	}
	return v
}

func getCapabilities(caps []apiv1.Capability) *apiv1.Capabilities {
	if len(caps) > 0 {
		return &apiv1.Capabilities{Add: caps}
	}
	return nil
}

func getSecurityContext(runAsOptions RunAsOptions) *apiv1.PodSecurityContext {
	secContext := &apiv1.PodSecurityContext{}
	if runAsOptions.RunAsUser != nil {
		secContext.RunAsUser = runAsOptions.RunAsUser
	}
	if runAsOptions.RunAsGroup != nil {
		secContext.RunAsGroup = runAsOptions.RunAsGroup
	}
	return secContext
}

// Helper function to get pod container status
func getPodContainerStatus(pod *apiv1.Pod) string {
	if pod == nil {
		return "unknown"
	}

	statuses := []string{}
	for _, cs := range pod.Status.ContainerStatuses {
		state := "unknown"
		if cs.State.Waiting != nil {
			state = fmt.Sprintf("%s (waiting: %s)", cs.Name, cs.State.Waiting.Reason)
		} else if cs.State.Running != nil {
			state = fmt.Sprintf("%s (running)", cs.Name)
		} else if cs.State.Terminated != nil {
			state = fmt.Sprintf("%s (terminated: %s)", cs.Name, cs.State.Terminated.Reason)
		}
		statuses = append(statuses, state)
	}

	if len(statuses) == 0 {
		return "no containers"
	}
	return fmt.Sprintf("[%s]", statuses)
}

// Helper function to get pod failure reason
func getPodFailureReason(pod *apiv1.Pod) string {
	if pod == nil {
		return "unknown"
	}

	reasons := []string{}

	// Check container statuses
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			reasons = append(reasons, fmt.Sprintf("container %s: %s (exit code: %d, message: %s)",
				cs.Name,
				cs.State.Terminated.Reason,
				cs.State.Terminated.ExitCode,
				cs.State.Terminated.Message))
		}
		if cs.State.Waiting != nil {
			reasons = append(reasons, fmt.Sprintf("container %s: %s (message: %s)",
				cs.Name,
				cs.State.Waiting.Reason,
				cs.State.Waiting.Message))
		}
	}

	// Check pod conditions
	for _, cond := range pod.Status.Conditions {
		if cond.Status == apiv1.ConditionFalse {
			reasons = append(reasons, fmt.Sprintf("condition %s: %s (message: %s)",
				cond.Type,
				cond.Reason,
				cond.Message))
		}
	}

	if len(reasons) == 0 {
		return pod.Status.Message
	}
	return fmt.Sprintf("[%s]", reasons)
}

func waitForService(u url.URL, t time.Duration) error {
	logger := logrus.WithFields(logrus.Fields{
		"function": "waitForService",
		"url":      u.String(),
		"timeout":  t,
	})

	logger.Info("Starting service readiness check")

	// Create custom HTTP client with proper timeouts
	httpClient := &http.Client{
		Timeout: 5 * time.Second, // Total request timeout
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second, // Connection timeout
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   2 * time.Second,
			ResponseHeaderTimeout: 3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConns:          10,
			IdleConnTimeout:       30 * time.Second,
		},
	}

	up := make(chan struct{})
	done := make(chan struct{})
	retryCount := 0
	maxRetries := int(t.Seconds() / 2) // Retry every 2 seconds

	logger.WithField("max_retries", maxRetries).Info("Calculated max retries")

	go func() {
		startTime := time.Now()
		for {
			select {
			case <-done:
				logger.WithField("retry_count", retryCount).Warn("Service check cancelled")
				return
			default:
			}

			retryCount++
			elapsed := time.Since(startTime)

			// Check /status endpoint (Selenium standard)
			statusURL := u.String() + "/status"
			req, err := http.NewRequest(http.MethodGet, statusURL, nil)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"retry":   retryCount,
					"elapsed": elapsed,
					"error":   err,
				}).Error("Failed to create HTTP request")
				<-time.After(2 * time.Second)
				continue
			}
			req.Close = true
			req.Header.Set("User-Agent", "selenosis-readiness-probe")

			logger.WithFields(logrus.Fields{
				"retry":    retryCount,
				"elapsed":  elapsed,
				"endpoint": statusURL,
			}).Debug("Attempting HTTP request")

			resp, err := httpClient.Do(req)
			if resp != nil {
				defer resp.Body.Close()
			}

			if err != nil {
				// Check if it's a DNS lookup error - wait longer for DNS propagation
				isDNSError := strings.Contains(err.Error(), "no such host") ||
					strings.Contains(err.Error(), "lookup") ||
					strings.Contains(err.Error(), "Temporary failure in name resolution")

				waitTime := 2 * time.Second
				if isDNSError && retryCount < 10 {
					// First 10 retries for DNS errors: wait 3 seconds (DNS propagation time)
					waitTime = 3 * time.Second
				}

				logger.WithFields(logrus.Fields{
					"retry":      retryCount,
					"elapsed":    elapsed,
					"error":      err.Error(),
					"is_dns_err": isDNSError,
					"wait_time":  waitTime,
				}).Warn("HTTP request failed, retrying...")
				<-time.After(waitTime)
				continue
			}

			logger.WithFields(logrus.Fields{
				"retry":       retryCount,
				"elapsed":     elapsed,
				"status_code": resp.StatusCode,
			}).Info("Received HTTP response")

			// Accept 200 OK or 204 No Content
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				logger.WithFields(logrus.Fields{
					"retry":      retryCount,
					"elapsed":    elapsed,
					"total_time": time.Since(startTime),
				}).Info("Service is ready!")
				up <- struct{}{}
				return
			}

			// Log unexpected status codes
			logger.WithFields(logrus.Fields{
				"retry":       retryCount,
				"elapsed":     elapsed,
				"status_code": resp.StatusCode,
			}).Warn("Unexpected status code, retrying...")

			<-time.After(2 * time.Second)
		}
	}()

	select {
	case <-time.After(t):
		close(done)
		logger.WithFields(logrus.Fields{
			"retry_count": retryCount,
			"timeout":     t,
		}).Error("Service readiness check timed out")
		return fmt.Errorf("service not ready after %v (tried %d times)", t, retryCount)
	case <-up:
		logger.Info("Service readiness check completed successfully")
	}
	return nil
}
