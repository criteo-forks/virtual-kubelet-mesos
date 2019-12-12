# Mesos Virtual Kubelet Provider

This virtual kubelet will register as a Mesos framework and schedule pods on a
Mesos cluster.

## Features

* Add pod
* Delete pod

This is a beta version. A lot of features are not implemented yet.

## Build

```
make clean build
```

## Usage

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
        "frameworkID": "dfd54ba6-200a-4cdf-b6ec-e77aa64a8f5f-0035",
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
