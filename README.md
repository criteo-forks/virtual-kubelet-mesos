# Mesos Virtual Kubelet Provider

This virtual kubelet will register as a Mesos framework and schedule pods on a
Mesos cluster.

## Features

* Add pod and convert PodSpec to Mesos TaskGroup
* Delete pod
* Update pod status based on Mesos TaskStatus events
* Recover after a restart by using the Kubelet UID as a Mesos FrameworkID

This is a beta version. A lot of features are not implemented yet.

## Known Missing Features

* Update a running pod
* Readiness/Liveness probes
* Container attach and logs
* CNI/networking
* GPU support
* Pod statistics summary
* Dynamic update of node (kubelet), especially capacity, currently  you must
  specify the total capacity in the configuration and the node conditions are
  hardcoded.
* Have a clean way to reconcile tasks upon startup

## Build

```
make clean build
```

## Usage

Properly set the `~/.kube/config` (using something like `minikube` for example,
and then:

```
bin/virtual-kubelet --privider mesos [...options]
```

## Configuration

Provider configuration file is passed via the `--provider-config` flag.

Example config:

```
{
    "scheduler": {
        "mesosURL": "http://mesos-master:5050/api/v1/scheduler",
        "role": "my-role",
        "principal": "my-principal",
        "user": "my-user",
        "authMode": "basic",
        "failoverTimeoutDuration": "1h",
        "credentials": {
            "username": "my-principal",
            "password": "badpassword"
        }
    }
}
```


## Credits

@pires and @jieyu for the initial implementation

(see https://github.com/jieyu/virtual-kubelet and
https://github.com/pires/virtual-kubelet)
