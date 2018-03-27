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
- Only one artifact to copy
- No external dependencies
- Standardized output in JSON and text format. Output can be put into database
- Modules:
    - Cluster audit
    - Network benchmark with iperf

## Setup locally

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



