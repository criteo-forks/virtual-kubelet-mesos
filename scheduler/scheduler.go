package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"time"

	mesos "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	xmetrics "github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"

	corev1 "k8s.io/api/core/v1"
)

var (
	RegistrationMinBackoff = 1 * time.Second
	RegistrationMaxBackoff = 15 * time.Second
)

type mesosScheduler struct {
	ready    chan struct{}
	config   *Config
	store    *stateStore
	notifier func(*corev1.Pod)
}

type Scheduler interface {
	// TODO @pires providers are stateless :(
	// Run() (quit chan<- struct{}, err error)
	Run(node *corev1.Node)
	WaitReady()
	AddPod(pod *corev1.Pod) error
	DeletePod(pod *corev1.Pod) error
	UpdatePod(pod *corev1.Pod) error
	GetPod(podNamespace, podName string) *corev1.Pod
	ListPods() []*corev1.Pod
	NotifyPods(context.Context, func(*corev1.Pod))
}

func New(config *Config) *mesosScheduler {
	return &mesosScheduler{
		ready:  make(chan struct{}),
		config: config,
		store:  newStateStore(config),
	}
}

func (sched *mesosScheduler) Run(node *corev1.Node) {
	// TODO log errors when they happen
	// TODO recover panics and rerun
	//defer func() {
	//	if r := recover(); r != nil {
	sched.run(node)
	//	}
	//}()
}

func (sched *mesosScheduler) WaitReady() {
	// Just wait 10 seconds
	// TODO: improve this
	timer := time.NewTimer(3 * time.Second)
	log.Println("Waiting 10 seconds for reconciliation...")
	<-timer.C
	log.Println("Scheduler is ready.")
}

func (sched *mesosScheduler) run(n *corev1.Node) error {
	log.Printf("Mesos scheduler running with configuration: %+v", sched.config)

	ctx, _ := context.WithCancel(context.Background())

	// TODO(jdef) how to track/handle timeout errors that occur for SUBSCRIBE calls? we should
	// probably tolerate X number of subsequent subscribe failures before bailing. we'll need
	// to track the lastCallAttempted along with subsequentSubscribeTimeouts.

	fidStore := store.DecorateSingleton(
		store.NewInMemorySingleton(),
		store.DoSet().AndThen(func(_ store.Setter, v string, _ error) error {
			log.Println("FrameworkID", v)
			return nil
		}))

	sched.store.cli = callrules.New(
		callrules.WithFrameworkID(store.GetIgnoreErrors(fidStore)),
		logAllCalls(),
		callMetrics(sched.store.metricsAPI, time.Now, true),
	).Caller(sched.store.cli)

	// Start reconciliation ticker
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go sched.reconcileTasks(ticker, done)

	err := controller.Run(
		ctx,
		buildFrameworkInfo(sched.store.config, n.UID),
		sched.store.cli,
		controller.WithEventHandler(buildEventHandler(sched.store, fidStore)),
		controller.WithFrameworkID(store.GetIgnoreErrors(fidStore)),
		controller.WithRegistrationTokens(
			backoff.Notifier(RegistrationMinBackoff, RegistrationMaxBackoff, ctx.Done()),
		),
		controller.WithSubscriptionTerminated(func(err error) {
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				return
			}
			log.Println("Scheduler disconnected")
		}),
	)

	if sched.store.err != nil {
		err = sched.store.err
	}
	return err
}

func (sched *mesosScheduler) reconcileTasks(ticker *time.Ticker, done chan struct{}) {
	log.Println("Start reconciliation ticker")
	reconcile(sched.store)
	for {
		select {
		case <-done:
			log.Println("Stop reconciliation ticker")
			return
		case <-ticker.C:
			reconcile(sched.store)
		}
	}
}

func (sched *mesosScheduler) AddPod(pod *corev1.Pod) error {
	sched.store.requestedPodMap.Set(buildPodNameFromPod(pod), &mesosPod{
		requestedPod: pod,
		taskStatuses: make(map[string]mesos.TaskStatus, len(pod.Spec.Containers)),
	})

	if sched.store.suppressed {
		tryReviveOffers(context.Background(), sched.store)
	}

	return nil
}
func (sched *mesosScheduler) DeletePod(pod *corev1.Pod) error {
	podKey := buildPodNameFromPod(pod)
	mesosPod, ok := sched.store.requestedPodMap.GetAndRemove(podKey)
	if ok {
		log.Printf("Pending pod %q has been deleted\n", podKey)
		return nil
	}

	mesosPod, ok = sched.store.unknownPodMap.GetAndRemove(podKey)
	if ok {
		log.Printf("Unknown pod %q will be deleted\n", podKey)
		shutdown, err := shutdownFromTaskStatus(mesosPod.taskStatuses)
		if err != nil {
			return err
		}
		return calls.CallNoData(context.Background(), sched.store.cli, shutdown)
	}

	mesosPod, ok = sched.store.runningPodMap.GetAndRemove(podKey)
	if ok {
		log.Printf("Sending shutdown call for running pod %q\n", podKey)
		sched.store.deletedPodMap.Set(podKey, mesosPod)
		shutdown, err := shutdownFromTaskStatus(mesosPod.taskStatuses)
		if err != nil {
			return err
		}
		return calls.CallNoData(context.Background(), sched.store.cli, shutdown)
	}

	mesosPod, ok = sched.store.deletedPodMap.Get(podKey)
	if ok {
		log.Printf("Sending shutdown call again for deleted pod %q\n", podKey)
		shutdown, err := shutdownFromTaskStatus(mesosPod.taskStatuses)
		if err != nil {
			return err
		}
		return calls.CallNoData(context.Background(), sched.store.cli, shutdown)
	}

	return fmt.Errorf("pod %q cannot be found\n", podKey)
}

func (sched *mesosScheduler) UpdatePod(pod *corev1.Pod) error {
	podKey := buildPodNameFromPod(pod)
	p, ok := sched.store.unknownPodMap.GetAndRemove(podKey)
	if !ok {
		return errors.New("Updating pod not supported")
	}
	p.requestedPod = pod
	sched.store.runningPodMap.Set(podKey, p)
	return nil
}

// Return an immutable deep copy of a pod
func (sched *mesosScheduler) GetPod(podNamespace, podName string) *corev1.Pod {
	podKey := buildPodNameFromStrings(podNamespace, podName)
	if pod, ok := sched.store.requestedPodMap.Get(podKey); ok {
		return pod.requestedPod
	}
	if pod, ok := sched.store.runningPodMap.Get(podKey); ok {
		return pod.requestedPod
	}
	if pod, ok := sched.store.deletedPodMap.Get(podKey); ok {
		return pod.requestedPod
	}

	return nil
}

func (sched *mesosScheduler) ListPods() []*corev1.Pod {
	//TODO: wait for first reconciliation?
	pods := make([]*corev1.Pod, sched.store.runningPodMap.Count()+sched.store.unknownPodMap.Count())
	for _, mesosPod := range sched.store.runningPodMap.Iter() {
		pods = append(pods, mesosPod.requestedPod)
	}
	for _, mesosPod := range sched.store.unknownPodMap.Iter() {
		pods = append(pods, buildDummyPodFromTaskStatuses(mesosPod.taskStatuses))
	}

	return pods
}

func (sched *mesosScheduler) NotifyPods(ctx context.Context, notifier func(*corev1.Pod)) {
	sched.store.notifier = notifier
}

func shutdownFromTaskStatus(taskStatuses map[string]mesos.TaskStatus) (*scheduler.Call, error) {
	for _, v := range taskStatuses {
		if v.ExecutorID != nil {
			return calls.Shutdown(v.ExecutorID.Value, v.AgentID.Value), nil
		}
	}
	return nil, errors.New("Empty map")
}

// buildEventHandler generates and returns a handler to process events received from the subscription.
func buildEventHandler(store *stateStore, fidStore store.Singleton) events.Handler {
	return eventrules.New(
		logAllEvents(),
		eventMetrics(store.metricsAPI, time.Now, true),
		controller.LiftErrors().DropOnError(),
	).Handle(events.Handlers{
		scheduler.Event_FAILURE:    executorFailure(),
		scheduler.Event_OFFERS:     trackOffersReceived(store).HandleF(resourceOffers(store)),
		scheduler.Event_UPDATE:     controller.AckStatusUpdates(store.cli).AndThen().HandleF(statusUpdate(store)),
		scheduler.Event_SUBSCRIBED: controller.TrackSubscription(fidStore, store.config.FailoverTimeout),
	})
}

func trackOffersReceived(store *stateStore) eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		if err == nil {
			store.metricsAPI.offersReceived.Int(len(e.GetOffers().GetOffers()))
		}
		return chain(ctx, e, err)
	}
}

func executorFailure() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		var (
			f              = e.GetFailure()
			eid, aid, stat = f.ExecutorID, f.AgentID, f.Status
		)
		if eid != nil {
			// executor failed..
			msg := "executor '" + eid.Value + "' terminated"
			if aid != nil {
				msg += " on agent '" + aid.Value + "'"
			}
			if stat != nil {
				msg += " with status=" + strconv.Itoa(int(*stat))
			}
			log.Println(msg)
		} else if aid != nil {
			// agent failed..
			log.Println("agent '" + aid.Value + "' terminated")
		}
		return chain(ctx, e, err)
	}
}

func resourceOffers(store *stateStore) events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		var (
			offers                 = e.GetOffers().GetOffers()
			callOption             = calls.RefuseSecondsWithJitter(rand.New(rand.NewSource(time.Now().Unix())), store.config.MaxRefuseSeconds)
			tasksLaunchedThisCycle = 0
			offersDeclined         = 0
			executorWantsResources = mesos.Resources{
				resources.NewCPUs(0.1).Resource,
				resources.NewMemory(32).Resource,
				resources.NewDisk(256).Resource,
				resources.Build().Name(resources.Name("network_bandwidth")).Scalar(float64(100)).Resource,
			}
		)

		store.suppressed = false

		for i := range offers {
			var (
				remainingOfferedResources = mesos.Resources(offers[i].Resources)
			)

			if store.config.Verbose {
				log.Printf("received offer id %q with resources %q\n", offers[i].ID.Value, remainingOfferedResources.String())
			}

			// decline if there are no new pods
			if store.requestedPodMap.Count() == 0 {
				log.Printf("no new pods. rejecting offer with id %q\n", offers[i].ID.Value)
				// send Reject call to Mesos
				reject := calls.Decline(offers[i].ID).With(callOption)
				err := calls.CallNoData(ctx, store.cli, reject)
				if err != nil {
					log.Printf("failed to reject offer with id %q. err %+v\n", offers[i].ID.Value, err)
				}
				offersDeclined++
				// TODO: backoff mechanism?
				suppressOffers(ctx, store)
				continue
			}

			firstNewPodName := store.requestedPodMap.Keys()[0]
			pod, _ := store.requestedPodMap.Get(firstNewPodName)

			flattened := remainingOfferedResources.ToUnreserved()

			// TODO @pires this only works if requests are defined
			taskGroupWantsResources := sumPodResources(pod.requestedPod)

			if store.config.Verbose {
				log.Printf("Pod %q wants the following resources %q", firstNewPodName, taskGroupWantsResources.String())
			}

			// decline if there offer doesn't fit pod (executor + tasks) resources request
			if !resources.ContainsAll(flattened, executorWantsResources.Plus(taskGroupWantsResources...)) ||
				!(offers[i].Hostname == "mesos-slave004-am6.central.criteo.preprod") {
				log.Printf("not enough resources in offer. rejecting offer with id %q\n", offers[i].ID.Value)
				// send Reject call to Mesos
				reject := calls.Decline(offers[i].ID).With(callOption)
				err := calls.CallNoData(ctx, store.cli, reject)
				if err != nil {
					log.Printf("failed to reject offer with id %q. err %+v\n", offers[i].ID.Value, err)
				}
				offersDeclined++
				continue
			}

			for name, restype := range resources.TypesOf(flattened...) {
				if restype == mesos.SCALAR {
					sum, _ := name.Sum(flattened...)
					store.metricsAPI.offeredResources(sum.GetScalar().GetValue(), name.String())
				}
			}

			if store.config.Verbose {
				log.Printf("Pod %q has %d containers\n", firstNewPodName, len(pod.requestedPod.Spec.Containers))
			}

			// Prepare executor
			executorInfo := buildDefaultExecutorInfo(offers[i].FrameworkID)
			found := func() mesos.Resources {
				return resources.Find(executorWantsResources, flattened...)
			}()
			executorInfo.ExecutorID = mesos.ExecutorID{Value: firstNewPodName}
			executorInfo.Resources = found

			remainingOfferedResources.Subtract(found...)
			flattened = remainingOfferedResources.ToUnreserved()

			// Build a TaskInfo for each container in the Kubernetes Pod.
			cap := len(pod.requestedPod.Spec.Containers)
			processedTasks := make([]mesos.TaskInfo, cap, cap)

			for pos, containerSpec := range pod.requestedPod.Spec.Containers {
				taskWantsResources := calculateTaskResources(containerSpec)

				if store.config.Verbose {
					log.Printf("Container %q wants resources %q", containerSpec.Name, taskWantsResources.String())
				}

				//if resources.ContainsAll(flattened, taskWantsResources) {
				found := func() mesos.Resources {
					return resources.Find(taskWantsResources, flattened...)
				}()

				if len(found) == 0 {
					log.Println("failed to find the resources that were supposedly contained")
				}

				if store.config.Verbose {
					log.Printf("launching pod %q using offer %q\n", firstNewPodName, offers[i].ID.Value)
				}

				task := buildPodTask(pod.requestedPod, &containerSpec)
				task.AgentID = offers[i].AgentID
				task.Resources = found
				processedTasks[pos] = task

				remainingOfferedResources.Subtract(found...)
				flattened = remainingOfferedResources.ToUnreserved()
				//}
			}

			taskGroupInfo := mesos.TaskGroupInfo{Tasks: processedTasks} // only needed for printing

			fmt.Printf("ExecutorInfo: %q\n", executorInfo.String())
			fmt.Printf("TaskGroupInfo: %q\n", taskGroupInfo.String())

			// build Accept call to launch all of the tasks we've assembled
			accept := calls.Accept(
				calls.OfferOperations{calls.OpLaunchGroup(executorInfo, processedTasks...)}.WithOffers(offers[i].ID),
			).With(callOption)

			// move pod to running pod
			pod, ok := store.requestedPodMap.GetAndRemove(firstNewPodName)
			if ok {
				store.runningPodMap.Set(firstNewPodName, pod)
			} else {
				log.Printf("failed to move pod %+v to runningpod map", pod)
			}

			// send Accept call to Mesos
			err := calls.CallNoData(ctx, store.cli, accept)
			if err != nil {
				log.Printf("failed to launch tasks: %+v", err)
				pod.requestedPod.Status.Phase = "Failed"
			} else {
				if n := len(processedTasks); n > 0 {
					tasksLaunchedThisCycle += n
				}
			}
		}

		store.metricsAPI.offersDeclined.Int(offersDeclined)
		store.metricsAPI.tasksLaunched.Int(tasksLaunchedThisCycle)
		store.metricsAPI.launchesPerOfferCycle(float64(tasksLaunchedThisCycle))
		if tasksLaunchedThisCycle == 0 && store.config.Verbose {
			log.Println("zero tasks launched this cycle")
		}
		return nil
	}
}

func statusUpdate(store *stateStore) events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		s := e.GetUpdate().GetStatus()
		if store.config.Verbose {
			msg := "Task " + s.TaskID.Value + " is in state " + s.GetState().String()
			if m := s.GetMessage(); m != "" {
				msg += " with message '" + m + "'"
			}
			log.Println(msg)
		}

		key, container := buildPodNameFromTaskStatus(&s)
		pod, ok := store.runningPodMap.Get(key)
		if !ok {
			pod, ok = store.deletedPodMap.Get(key)
			//TODO: remove pod when all executor is stopped?
		}
		if ok {
			// Pod already running, update or append status
			pod.taskStatuses[container] = s
			pod.requestedPod = updatePodFromTaskStatus(pod.requestedPod, container, s)
			log.Println("Notifying Pod Status update for " + key)
			store.notifier(pod.requestedPod)
			return nil
		} else {
			pod, ok = store.unknownPodMap.Get(key)
			if ok {
				pod.taskStatuses[container] = s
			} else {
				switch s.GetState() {
				case mesos.TASK_RUNNING, mesos.TASK_STAGING, mesos.TASK_STARTING:
					pod = &mesosPod{
						taskStatuses: make(map[string]mesos.TaskStatus, 1),
					}
					pod.taskStatuses[container] = s
					store.unknownPodMap.Set(key, pod)
				default:
					return errors.New("Failed/Terminated unknown Task ID: " + s.TaskID.Value)
				}
			}
			log.Println("Notifying Pod Status update for unknown pod " + key)
			store.notifier(buildDummyPodFromTaskStatuses(pod.taskStatuses))
			return nil
		}
	}
}

func suppressOffers(ctx context.Context, store *stateStore) {
	if !store.suppressed {
		log.Println("suppressing offers")
		store.suppressed = true
		if err := calls.CallNoData(ctx, store.cli, calls.Suppress()); err != nil {
			log.Printf("failed to suppress offers: %+v", err)
		}
	}
	return
}

func tryReviveOffers(ctx context.Context, store *stateStore) {
	// limit the rate at which we request offer revival
	select {
	case <-store.reviveTokens:
		// not done yet, revive offers!
		err := calls.CallNoData(ctx, store.cli, calls.Revive())
		if err != nil {
			log.Printf("failed to revive offers: %+v", err)
			return
		}
	default:
		// noop
	}
}

func reconcile(store *stateStore) {
	calls.CallNoData(context.Background(), store.cli, calls.Reconcile())
}

// eventMetrics logs metrics for every processed API event
func eventMetrics(metricsAPI *metricsAPI, clock func() time.Time, timingMetrics bool) eventrules.Rule {
	timed := metricsAPI.eventReceivedLatency
	if !timingMetrics {
		timed = nil
	}
	harness := xmetrics.NewHarness(metricsAPI.eventReceivedCount, metricsAPI.eventErrorCount, timed, clock)
	return eventrules.Metrics(harness, nil)
}

// callMetrics logs metrics for every outgoing Mesos call
func callMetrics(metricsAPI *metricsAPI, clock func() time.Time, timingMetrics bool) callrules.Rule {
	timed := metricsAPI.callLatency
	if !timingMetrics {
		timed = nil
	}
	harness := xmetrics.NewHarness(metricsAPI.callCount, metricsAPI.callErrorCount, timed, clock)
	return callrules.Metrics(harness, nil)
}

func logAllEvents() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, ch eventrules.Chain) (context.Context, *scheduler.Event, error) {
		message := ""
		switch e.Type {
		case scheduler.Event_SUBSCRIBED:
			message = fmt.Sprint("FrameworkID %s", e.GetSubscribed().GetFrameworkID().Value)
		case scheduler.Event_OFFERS:
			message = fmt.Sprintf("%d offers", len(e.GetOffers().GetOffers()))
		case scheduler.Event_RESCIND:
			message = e.GetRescind().GetOfferID().Value
		case scheduler.Event_UPDATE:
			s := e.GetUpdate().GetStatus()
			message = fmt.Sprintf("%s for task %s", s.GetState(), s.GetTaskID())
		case scheduler.Event_MESSAGE:
			message = fmt.Sprintf("%s", e.GetMessage().GetData())
		case scheduler.Event_FAILURE:
			f := e.GetFailure()
			message = fmt.Sprintf("agent %s executor %s", f.AgentID.GetValue(), f.ExecutorID.GetValue())
		case scheduler.Event_ERROR:
			message = e.GetError().GetMessage()
		}
		log.Println("event", e.Type, message)
		return ch(ctx, e, err)
	}
}

func logAllCalls() callrules.Rule {
	return func(ctx context.Context, c *scheduler.Call, r mesos.Response, err error, ch callrules.Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		message := ""
		switch c.GetType() {
		case scheduler.Call_ACCEPT:
			for _, o := range c.GetAccept().GetOperations() {
				if message != "" {
					message += ", "
				}
				message += o.GetType().String()
				if o.GetType() == mesos.Offer_Operation_LAUNCH_GROUP {
					message += " " + o.GetLaunchGroup().GetExecutor().ExecutorID.Value
				}
			}
		}
		log.Println("call", c.GetType(), message)
		return ch(ctx, c, r, err)
	}
}
