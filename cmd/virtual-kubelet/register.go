package main

import (
	"github.com/virtual-kubelet/mesos/cmd/virtual-kubelet/internal/provider"
	"github.com/virtual-kubelet/mesos/cmd/virtual-kubelet/internal/provider/mesos"
)

func registerMesos(s *provider.Store) {
	s.Register("mesos", func(cfg provider.InitConfig) (provider.Provider, error) { //nolint:errcheck
		return mesos.NewMesosProvider(
			cfg.ConfigPath,
			cfg.NodeName,
			cfg.OperatingSystem,
			cfg.InternalIP,
			cfg.DaemonPort,
		)
	})
}
