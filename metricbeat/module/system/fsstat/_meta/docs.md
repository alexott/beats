The System `fsstat` metricset provides overall file system statistics.

This metricset is available on:

* FreeBSD
* Linux
* macOS
* OpenBSD
* Windows


## Configuration [_configuration_8]

**`filesystem.ignore_types`** - A list of filesystem types to ignore. Metrics will not be collected from filesystems matching these types. This setting also affects the `filesystem` metricset. If this option is not set, metricbeat ignores all types for virtual devices in systems where this information is available (e.g. all types marked as `nodev` in `/proc/filesystems` in Linux systems).
