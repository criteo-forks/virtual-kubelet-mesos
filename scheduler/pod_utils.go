package scheduler

import (
	"strings"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/resources"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultContainerCPU  = float64(0.1) // 0.1 = 100 milliseconds of a second
	defaultContainerMem  = float64(32)  // 32 MB
	defaultContainerDisk = float64(128) // 128 MB
)

// calculateTaskResources calculates the resources needed for a container.
func calculateTaskResources(container corev1.Container) mesos.Resources {

	containerCPU := float64(container.Resources.Requests.Cpu().MilliValue() / 1000)
	if containerCPU < defaultContainerCPU {
		containerCPU = defaultContainerCPU
	}

	containerMem := float64(container.Resources.Requests.Memory().Value() / 1024 / 1024)
	if containerMem < defaultContainerMem {
		containerMem = defaultContainerMem
	}

	containerDisk := float64(container.Resources.Requests.StorageEphemeral().MilliValue() / 1000)
	if containerDisk < defaultContainerDisk {
		containerDisk = defaultContainerDisk
	}

	return mesos.Resources{
		resources.NewCPUs(containerCPU).Resource,
		resources.NewMemory(containerMem).Resource,
		resources.NewDisk(containerDisk).Resource,
		resources.Build().Name(resources.Name("network_bandwidth")).Scalar(float64(100)).Resource, // 100 Mbps
	}
}

// sumPodResources sums all the resources required for a Pod.
// Enforce default value if resource == 0
func sumPodResources(pod *corev1.Pod) mesos.Resources {

	var podResources mesos.Resources
	for _, containerSpec := range pod.Spec.Containers {
		containerResources := calculateTaskResources(containerSpec)
		podResources = podResources.Plus(containerResources...)
	}

	return podResources
}

// buildPodTask creates a new Mesos Task based on a Kubernetes pod container definition.
func buildPodTask(pod *corev1.Pod, containerSpec *corev1.Container) mesos.TaskInfo {
	taskId := mesos.TaskID{Value: containerSpec.Name}

	// Build task environment variables.
	var taskEnvVars []mesos.Environment_Variable
	for _, envVar := range containerSpec.Env {
		taskEnvVar := mesos.Environment_Variable{Name: envVar.Name, Value: proto.String(envVar.Value)}
		taskEnvVars = append(taskEnvVars, taskEnvVar)
	}

	// Build TaskInfo.
	task := mesos.TaskInfo{
		TaskID: taskId,
		Container: &mesos.ContainerInfo{
			Type: mesos.ContainerInfo_MESOS.Enum(),
			Mesos: &mesos.ContainerInfo_MesosInfo{
				Image: &mesos.Image{
					Type: mesos.Image_DOCKER.Enum(),
					Docker: &mesos.Image_Docker{
						Name: containerSpec.Image,
					},
				},
			},
		},
		// TODO @pires build command info properly based on podSpec.Command, podSpec.Args, etc.
		Command: &mesos.CommandInfo{
			Shell: proto.Bool(false),
			//Value:     proto.String(strings.Join(containerSpec.Command, " ")),
			//Arguments: containerSpec.Args,
			Environment: &mesos.Environment{
				Variables: taskEnvVars,
			},
		},
	}
	return task
}

func mesosStateToContainerState(status mesos.TaskStatus) corev1.ContainerState {
	switch status.GetState() {
	case mesos.TASK_RUNNING:
		return corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}
	case
		mesos.TASK_FINISHED,
		mesos.TASK_FAILED,
		mesos.TASK_KILLED,
		mesos.TASK_ERROR,
		mesos.TASK_LOST,
		mesos.TASK_DROPPED,
		mesos.TASK_UNREACHABLE,
		mesos.TASK_GONE,
		mesos.TASK_GONE_BY_OPERATOR:
		return corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				Reason:      reasonCamelCase(status.GetReason()),
				Message:     status.GetMessage(),
				ContainerID: status.GetContainerStatus().GetContainerID().GetValue(),
			},
		}
	default:
	}
	return corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  reasonCamelCase(status.GetReason()),
			Message: status.GetMessage(),
		},
	}
}

func reasonCamelCase(reason mesos.TaskStatus_Reason) string {
	r := ""
	for _, s := range strings.Split(strings.TrimPrefix(reason.String(), "REASON_"), "_") {
		r += s[0:1] + strings.ToLower(s[1:])
	}
	return r
}

func buildDummyPodFromTaskStatuses(taskStatuses map[string]mesos.TaskStatus) *corev1.Pod {
	if len(taskStatuses) == 0 {
		return nil
	}
	containers := make([]corev1.Container, 0, len(taskStatuses))
	containerStatuses := make([]corev1.ContainerStatus, 0, len(taskStatuses))

	podNamespace, podName := "", ""

	for name, task := range taskStatuses {
		podNamespace, podName = splitPodKeyName(task.GetExecutorID().GetValue())
		container := corev1.Container{
			Name: name,
		}
		containers = append(containers, container)
		containerStatus := corev1.ContainerStatus{
			Name:  name,
			State: mesosStateToContainerState(task),
		}
		containerStatuses = append(containerStatuses, containerStatus)
	}

	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
		},
		Spec: corev1.PodSpec{
			Volumes:    []corev1.Volume{},
			Containers: containers,
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: containerStatuses,
		},
	}
}

func updatePodFromTaskStatus(pod *corev1.Pod, status mesos.TaskStatus) *corev1.Pod {
	containerStatus := corev1.ContainerStatus{
		Name:  status.GetTaskID().Value,
		State: mesosStateToContainerState(status),
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		pod.Status.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	}
	statusUpdated := false
	for i, s := range pod.Status.ContainerStatuses {
		if s.Name == containerStatus.Name {
			pod.Status.ContainerStatuses[i] = containerStatus
			statusUpdated = true
		}
	}
	if !statusUpdated {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, containerStatus)
	}
	failed := false
	for _, s := range pod.Status.ContainerStatuses {
		if s.State.Terminated != nil {
			if s.State.Terminated.Reason == "Finished" {
				continue
			} else {
				failed = true
			}
		}
		if s.State.Running != nil {
			pod.Status.Phase = corev1.PodRunning
			return pod
		}
		if s.State.Waiting != nil {
			pod.Status.Phase = corev1.PodPending
			return pod
		}
	}
	if failed {
		pod.Status.Phase = corev1.PodFailed
		return pod
	} else {
		pod.Status.Phase = corev1.PodSucceeded
		return pod
	}
}

// buildDefaultExecutorInfo returns the protof of a default executor.
func buildDefaultExecutorInfo(fid mesos.FrameworkID) mesos.ExecutorInfo {
	return mesos.ExecutorInfo{
		Type:        mesos.ExecutorInfo_DEFAULT,
		FrameworkID: &fid,
		Container: &mesos.ContainerInfo{
			Type: mesos.ContainerInfo_MESOS.Enum(),
			NetworkInfos: []mesos.NetworkInfo{
				{
					IPAddresses: []mesos.NetworkInfo_IPAddress{{}},
					//Name:        proto.String("dcos"), // TODO @pires configurable CNI
				},
			},
		},
	}
}

// buildPodNameFromPod is a helper for building the "key" for the providers pod store.
func buildPodNameFromPod(pod *corev1.Pod) string {
	return buildPodNameFromStrings(pod.GetNamespace(), pod.GetName())
}

func buildPodNameFromStrings(namespace string, name string) string {
	return namespace + "=" + name
}

func buildPodNameFromTaskStatus(status *mesos.TaskStatus) string {
	//n, p, _ := cache.SplitMetaNamespaceKey(status.GetExecutorID().GetValue())
	return status.GetExecutorID().GetValue()
}

func splitPodKeyName(key string) (string, string) {
	parts := strings.Split(key, "=")
	return parts[0], parts[1]
}
