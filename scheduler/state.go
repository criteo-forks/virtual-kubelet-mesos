package scheduler

import (
	"log"

	mesos "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func buildMesosCaller(schedulerConfig *Config) calls.Caller {
	var authConfigOpt httpcli.ConfigOpt
	if schedulerConfig.AuthMode == AuthModeBasic {
		log.Println("configuring HTTP Basic authentication")
		authConfigOpt = httpcli.BasicAuth(schedulerConfig.Credentials.Username, schedulerConfig.Credentials.Password)
	}

	cli := httpcli.New(
		httpcli.Endpoint(schedulerConfig.MesosURL),
		httpcli.Codec(schedulerConfig.Codec.Codec),
		httpcli.Do(httpcli.With(
			authConfigOpt,
			httpcli.Timeout(schedulerConfig.Timeout),
		)),
	)
	return httpsched.NewCaller(cli, httpsched.Listener(func(n httpsched.Notification) {
		if schedulerConfig.Verbose {
			log.Printf("scheduler client notification: %+v", n)
		}
	}))
}

func buildFrameworkInfo(schedulerConfig *Config, uid types.UID) *mesos.FrameworkInfo {
	failoverTimeout := schedulerConfig.FailoverTimeout.Seconds()
	frameworkInfo := &mesos.FrameworkInfo{
		ID:         &mesos.FrameworkID{Value: string(uid)},
		User:       schedulerConfig.User,
		Name:       schedulerConfig.Name,
		Checkpoint: &schedulerConfig.Checkpoint,
		Capabilities: []mesos.FrameworkInfo_Capability{
			{Type: mesos.FrameworkInfo_Capability_RESERVATION_REFINEMENT},
		},
	}
	if schedulerConfig.FailoverTimeout > 0 {
		frameworkInfo.FailoverTimeout = &failoverTimeout
	}
	if schedulerConfig.Role != "" {
		frameworkInfo.Role = &schedulerConfig.Role
	}
	if schedulerConfig.Principal != "" {
		frameworkInfo.Principal = &schedulerConfig.Principal
	}

	if schedulerConfig.Hostname != "" {
		frameworkInfo.Hostname = &schedulerConfig.Hostname
	}

	if len(schedulerConfig.Labels) > 0 {
		log.Println("using labels:", schedulerConfig.Labels)
		frameworkInfo.Labels = &mesos.Labels{Labels: schedulerConfig.Labels}
	}
	// TODO gpu?
	//if schedulerConfig.gpuClusterCompat {
	//	frameworkInfo.Capabilities = append(frameworkInfo.Capabilities,
	//		mesos.FrameworkInfo_Capability{Type: mesos.FrameworkInfo_Capability_GPU_RESOURCES},
	//	)
	//}
	return frameworkInfo
}

func newStateStore(schedulerConfig *Config) *stateStore {
	return &stateStore{
		config:          schedulerConfig,
		cli:             buildMesosCaller(schedulerConfig),
		reviveTokens:    backoff.BurstNotifier(schedulerConfig.ReviveBurst, schedulerConfig.ReviveWait, schedulerConfig.ReviveWait, nil),
		metricsAPI:      initMetrics(*schedulerConfig),
		requestedPodMap: NewMesosPodMap(),
		unknownPodMap:   NewMesosPodMap(),
		runningPodMap:   NewMesosPodMap(),
		deletedPodMap:   NewMesosPodMap(),
		suppressed:      false,
	}
}

type stateStore struct {
	role            string
	cli             calls.Caller
	config          *Config
	reviveTokens    <-chan struct{}
	metricsAPI      *metricsAPI
	err             error
	requestedPodMap *MesosPodMap
	unknownPodMap   *MesosPodMap
	runningPodMap   *MesosPodMap
	deletedPodMap   *MesosPodMap
	suppressed      bool
	notifier        func(*corev1.Pod)
}
