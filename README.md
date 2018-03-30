# MapR Clustercheck Suite

This cluster check suite is intented to check against a current cluster installations.
The major objective is to provide a standardized way for PS to provide cluster audits.
Furthermore the idea is to collect data from customer clusters and put them into a centralied database.

## Tracking

[Tasks, Features and Issues](https://github.com/mapr-emea/mapr-clustercheck/issues)

[Specification Phase](https://github.com/mapr-emea/mapr-clustercheck/projects/2)

[Release v0.1](https://github.com/mapr-emea/mapr-clustercheck/projects/3)


## Features

- Run easily cluster checks with only one command
- Support following authentication methods:
    - SSH authentication with identity key
    - SSH authentication with password
    - Root users
    - Users with sudo
    - Sudo with password
- Only one artifact to copy
- No external dependencies
- Standardized output in JSON and text format. Output can be put into database
- Modules:
    - Cluster Audit
    - Cluster Config Audit
    - Benchmark Memory
    - Benchmark Raw Disks (dd for readonly, iozone for destructive)
    - Benchmark Raw Disks (dd for readonly, iozone for destructive)
    - Benchmark Network with iperf
    - Benchmark DFSIO, cluster disk performance
    - Benchmark RW test from single node (local volumes / standard volumes)

## How to use

TODO setup java. yum install java-1.8.0-openjdk
TODO How to generate template
TODO config nodes with passwd or key
TODO testssh
TODO validate
TODO run

scp maprclustercheck username@123.123.123.123:~
chmod +x maprclustercheck

./maprclustercheck
USAGE:
  Show included modules and versions:      ./maprclustercheck info
  Run checks:                              ./maprclustercheck run /path/to/myconfig.yaml
  Tests SSH connections:                   ./maprclustercheck testssh /path/to/myconfig.yaml
  Validate configuration file:             ./maprclustercheck validate /path/to/myconfig.yaml
  Create configuration template:           ./maprclustercheck generatetemplate /path/to/myconfig.yaml



----
./maprclustercheck info
...

2018-Mar-30 10:08:34 - INFO Included modules:
2018-Mar-30 10:08:34 - INFO > cluster-audit -> 1.0
2018-Mar-30 10:08:34 - INFO > cluster-config-audit -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-rawdisk -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-memory -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-network-iperf -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-maprfs-dfsio -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-maprfs-rwtest -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-yarn-terasort-mr -> 1.0
2018-Mar-30 10:08:34 - INFO > benchmark-yarn-terasort-spark -> 1.0

./maprclustercheck generatetemplate clustername.yml
...
2018-Mar-30 10:09:50 - INFO >>> Configuration template written to /home/mapr/mycluster.yml

cluster_name: demo.mapr.com
customer_name: Your company name
output_dir: /path/for/results
mapr_user: mapr
nodes-global-config:
  disks:
  - /dev/nvme1n1
  - /dev/nvme2n1
  - /dev/nvme3n1
  ssh_user: ec2-user
  ssh_identity: /Users/chufe/.ssh/id_rsa
  ssh_port: 22
nodes:
- host: hostname1.fqdn
  roles:
  - clusterjob-execution
- host: hostname2.fqdn
- host: hostname3.fqdn
  ssh_user: different_user
  ssh_identity: /home/user/.ssh/different_key
  ssh_port: 22222
modules:
  cluster-audit:
    enabled: true
    mapruser: mapr
...

./maprclustercheck testssh mycluster.yml
...
2018-Mar-30 10:12:37 - INFO Number of modules found: 9
2018-Mar-30 10:12:39 - INFO Connection to all nodes is working properly.

 ./maprclustercheck validate mycluster.yml
2018-Mar-30 10:13:08 - INFO Number of modules found: 9
2018-Mar-30 10:13:10 - INFO ====== Starting validation ======
2018-Mar-30 10:13:10 - INFO Validating cluster-audit - 1.0
2018-Mar-30 10:13:11 - INFO Validating cluster-config-audit - 1.0
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-rawdisk, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-memory, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-network-iperf, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-maprfs-dfsio, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-maprfs-rwtest, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-yarn-terasort-mr, because it is disabled.
2018-Mar-30 10:13:11 - INFO >>> Skipping module benchmark-yarn-terasort-spark, because it is disabled.
2018-Mar-30 10:13:11 - INFO ====== Validation finished ======
2018-Mar-30 10:13:11 - INFO >>> Everything is good. You can start cluster checks

Might be possible that it shows something like "Please install following tools".

 ./maprclustercheck run mycluster.yml
2018-Mar-30 10:15:13 - INFO Number of modules found: 9
2018-Mar-30 10:15:16 - INFO ====== Starting validation ======
2018-Mar-30 10:15:16 - INFO Validating cluster-audit - 1.0
2018-Mar-30 10:15:18 - INFO Validating cluster-config-audit - 1.0
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-rawdisk, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-memory, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-network-iperf, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-maprfs-dfsio, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-maprfs-rwtest, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-yarn-terasort-mr, because it is disabled.
2018-Mar-30 10:15:18 - INFO >>> Skipping module benchmark-yarn-terasort-spark, because it is disabled.
2018-Mar-30 10:15:18 - INFO ====== Validation finished ======
2018-Mar-30 10:15:18 - INFO ... Creating output directory /home/mapr/clusteraudit/2018-03-30-10-15-18
2018-Mar-30 10:15:18 - INFO ====== Starting execution =======
2018-Mar-30 10:15:18 - INFO Executing cluster-audit - 1.0
2018-Mar-30 10:15:18 - INFO >>>>> Running cluster-audit
2018-Mar-30 10:15:32 - INFO >>>>> ... cluster-audit finished
2018-Mar-30 10:15:32 - INFO ... Writing summary JSON result: /home/mapr/clusteraudit/2018-03-30-10-15-18/cluster-audit.json
2018-Mar-30 10:15:32 - INFO ... Writing summary TEXT report: /home/mapr/clusteraudit/2018-03-30-10-15-18/cluster-audit.txt
2018-Mar-30 10:15:32 - INFO Executing cluster-config-audit - 1.0

....
## Setup project locally

The build requires Gradle (www.gradle.org). If you have a MacBook and brew installed, just run the following command to install Gradle

```
$ brew install gradle
```

Clone the project from GitHub

```
$ git clone git@github.com:mapr-emea/mapr-clustercheck.git
```

Generate IDEA IntelliJ project files

```
$ cd mapr-clustercheck
$ gradle idea
```

Open with IntelliJ. Might be a good idea to disable Gradle plugin in IntelliJ, because it slows down.

Build the project

```
$ cd mapr-clustercheck
$ gradle clean build
```

Final executable artifact can be found in this path:

```
build/clustercheck-suite
```

## Install on cluster

Copy artifact `build/clustercheck-suite` to remote cluster.

```
$ scp build/clustercheck-suite ec2-user@10.0.0.123:~
```

Make artifact executable (connect with SSH to node)

```
$ chmod +x ~/clustercheck-suite
```

## Run on cluster

```
$ ./maprclustercheck

USAGE: 
  Show included modules and versions:      ./maprclustercheck info
  Run checks:                              ./maprclustercheck run /path/to/myconfig.yaml
  Tests SSH connections:                   ./maprclustercheck testssh /path/to/myconfig.yaml
  Validate configuration file:             ./maprclustercheck validate /path/to/myconfig.yaml
  Create configuration template:           ./maprclustercheck generatetemplate /path/to/myconfig.yaml

```

Based on the bundled modules the checks are executed. It is helpful to generate a configuration file template with `clusterchecksuite generatetemplate`, because this takes all bundled modules into account and generates a configuration file based on this.

## Project structure

- `clustercheck-core` contains the base framework
- `clustercheck-suite` builds and bundles the final artifact
- `modules` contains all the modules, which are bundled with `clustercheck-suite`
- `playground` place your test stuff here.

## Development Guidelines

- Keep everything simple
- Every module must be able to run out of the box with standard configuration
- Do NOT rely on external dependencies. Everything should be delivered with the final artifact and automatically rolled out. E.g. Ansible or Python 3.5 are not a preriquisite.
- The suite must run out of the box on all MapR supported platforms.



