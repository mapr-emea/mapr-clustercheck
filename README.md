# MapR Clustercheck Suite

This cluster check suite is intented to check against a current cluster installations.
The major objective is to provide a standardized way for PS to provide cluster audits.
Furthermore the idea is to collect data from customer clusters and put them into a centralied database.

## Features

- Run easily cluster checks with only one command
- Support all common authentication methods:
    - SSH authentication with identity key
    - SSH authentication with password
    - Root users
    - Users with sudo
    - Sudo with password
- Only one artifact to copy
- Gives you recommendations for nodes and cluster
- No external dependencies, e.g. no clustershell is required.
- Standardized output in JSON and text format. Output can be put into database.
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

### Prerequites

The artifact only needs to be copied to one node. This tool requires Java. If Java is not installed you can use OpenJDK or Oracle JDK.

Download latest from: https://drive.google.com/drive/u/0/folders/1NARtRJbbUc7Aw5tBZ_0xYxiQk4emXnY9

Collect results in: https://drive.google.com/drive/u/0/folders/1jMfOQCopAmXqPlFR2aEi0xlLzB9jR09L

Ensure that no passwords are in reports.

Install Java on Redhat:

```
$ yum install java-1.8.0-openjdk
```

Upload to one node:

```
$ scp maprclustercheck username@123.123.123.123:~
```

Connect to node:

```
$ ssh username@123.123.123.123
```

Make it executable./maprclustercheck
                  USAGE:
                    Show included modules and versions:      ./maprclustercheck info
                    Run checks:                              ./maprclustercheck run /path/to/myconfig.yaml
                    Tests SSH connections:                   ./maprclustercheck testssh /path/to/myconfig.yaml
                    Validate configuration file:             ./maprclustercheck validate /path/to/myconfig.yaml
                    Create configuration template:           ./maprclustercheck generatetemplate /path/to/myconfig.yaml

```
$ chmod +x maprclustercheck
```

Display help

```
$ ./maprclustercheck
USAGE:
  Show included modules and versions:      ./maprclustercheck info
  Run checks:                              ./maprclustercheck run /path/to/myconfig.yaml
  Tests SSH connections:                   ./maprclustercheck testssh /path/to/myconfig.yaml
  Validate configuration file:             ./maprclustercheck validate /path/to/myconfig.yaml
  Create configuration template:           ./maprclustercheck generatetemplate /path/to/myconfig.yaml
```

Display bundled modules (Cluster audit can be bundled with different modules` below is the default)

```
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
```

### Creating a template

You can easily generate a template, based on the bundled modules

```
$ ./maprclustercheck generatetemplate clustername.yml
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
  benchmark-yarn-terasort-spark:
    enabled: true
    role: clusterjob-execution
    tests:
    - chunk_size_in_mb: 256
      data_size: 1024M
      num_executors: 3
      executor_cores: 2
      executor_memory: 4G
      topology: /data
      replication: 3
      compression: 'on'
...
```

This template contains default-settings for all modules and activates them by default.

In `modules` block, you can change the settings for the modules and disable them if necessary.
In `nodes-global-config` block, you can provide settings which apply to all nodes, like authentication settings,
if you have different settings for some nodes like disks or a different SSH authentication, port, etc,
you can override these settings in `nodes`block for a specfic node.
In `nodes` there is also a roles block. For some modules it required to define a role, e.g.
when you run a TeraSort, this is a job which usually will be submitted on only one node, so you can mark a node with a role `clusterjob-execution`
and tell the module for TeraSort that it should use this role, so execute this job only on the node with the defined role.

*Global properties*

Please adjust the following properties. The `output_dir` contains the results for all audits. This means, an existing result will never be overwritten.

```
cluster_name: demo.mapr.com
customer_name: Your company name
output_dir: /path/for/results
```

*SSH authentication*

For SSH authentication with key, you can use:

```
nodes-global-config:
  disks:
    ....
  ssh_user: ec2-user
  ssh_identity: /Users/chufe/.ssh/id_rsa
  ssh_port: 22
```

For SSH authentication with password, you can use (the same password will be used for sudo password request)

```
nodes-global-config:
  disks:
    ....
  ssh_user: ec2-user
  ssh_password: my_super_secret_password
  ssh_port: 22
```

After everything is configured properly, you can test the SSH connection:

```
$ /maprclustercheck testssh mycluster.yml
```

If it cannot connect to one node it will display an error like

```
2018-Mar-30 11:17:59 -ERROR Unable to connect to following hosts: [10.0.0.63, 10.0.0.115, 10.0.0.192]
```

A successfull SSH test connection looks like

```
2018-Mar-30 10:12:39 - INFO Connection to all nodes is working properly.
```

The SSH check is always done before the job runs, if it is not possible to connect to one node, the entire check will not run.

After SSH connection was checked successfully, you can validate your configuration

```
$ ./maprclustercheck validate mycluster.yml
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
```

There are two types for validation. Warnings and errors:

* With warnings you are still allowed to the validation, nevertheless you should get rid of warnings
* If you have errors, it will not be possible to start the validation.

After validation was successful, you can start the cluster validation

```
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
...
```

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



