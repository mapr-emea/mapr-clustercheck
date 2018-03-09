package com.mapr.emea.ps.clustercheck.module.benchmark.yarn.terasort.spark

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import groovy.json.JsonSlurper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

/**
 * Created by chufe on 22.08.17.
 */
// TODO option reuse volume
// TODO option reuse data
// TODO option custom volumename /dynamic
@ClusterCheckModule(name = "benchmark-yarn-terasort-spark", version = "1.0")
class BenchmarkYarnTerasortSparkModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkYarnTerasortSparkModule.class);

    @Autowired
    @Qualifier("ssh")
    def ssh
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    ResourceLoader resourceLoader;

    @Override
    Map<String, ?> yamlModuleProperties() {
        return [role: "clusterjob-execution", tests: [["chunk_size_in_mb": 256, data_size: "1T", num_executors: 10, executor_cores: 2, executor_memory: "4G", "topology": "/data", replication: 3, compression: "on"]]]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-spark'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        if (role == "all") {
            throw new ModuleValidationException("Please specify a role for 'benchmark-maprfs'-module which is not 'all'. Usually it should run only on one node.")
        }
        return []
        // TODO validate spark version 2.1
        // TODO check for valid ticket, if secure cluster
        // TODO check that role has only one node inside
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-spark'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        deleteBenchmarkVolume(moduleconfig, role)
        def result = []
        def sparkTeraJar = copySparkTeraSortToRemoteHost(role)
        def tests = moduleconfig.tests
        for (def test  : tests) {
            setupBenchmarkVolume(test, role)
            def teraGenResult = generateTerasortBenchmark(test, role, sparkTeraJar)
            def teraSortResult = runTerasortBenchmark(test, role, sparkTeraJar)
            result << [teraGen: teraGenResult, teraSort: teraSortResult]
            deleteBenchmarkVolume(test, role)
        }
        return new ClusterCheckResult(reportJson: result, reportText: "not yet implemented", recommendations: [])
    }


    def copySparkTeraSortToRemoteHost(role) {
        log.info(">>>>> Copy SparkTeraSort.jar to remote host")
        def sparkTeraJar = ""
        ssh.run {
            session(ssh.remotes.role(role)) {
                def inputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/yarn/terasort/spark/spark-2.1-terasort-1.1-SNAPSHOT.jar").getInputStream()
                put from: inputStream, into: "/tmp/spark-2.1-terasort-1.1-SNAPSHOT.jar"
                def homePath = executeSudo "su ${globalYamlConfig.mapr_user} -c 'echo \$HOME'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'mkdir -p ${homePath}/.clustercheck'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'cp /tmp/spark-2.1-terasort-1.1-SNAPSHOT.jar ${homePath}/.clustercheck/spark-2.1-terasort-1.1-SNAPSHOT.jar'"
                sparkTeraJar = "${homePath}/.clustercheck/spark-2.1-terasort-1.1-SNAPSHOT.jar"
            }
        }
        return sparkTeraJar
    }


    def setupBenchmarkVolume(Map<String, ?> moduleconfig, role) {
        log.info(">>>>> Creating /benchmarks volume.")
        def topology = moduleconfig.getOrDefault("topology", "/data")
        def replication = moduleconfig.getOrDefault("replication", 1)
        def compression = moduleconfig.getOrDefault("compression", "on")
        def chunkSize = moduleconfig.getOrDefault("chunkSizeInMB", 256) * 1024

        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def topologyStr = topology != "/data" ? "-topology ${topology}" : ""
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume create -name benchmarks -path /benchmarks -replication ${replication} ${topologyStr}'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -chmod 777 /benchmarks'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -mkdir /benchmarks/tera'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop mfs -setcompression ${compression} /benchmarks'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop mfs -setchunksize ${chunkSize} /benchmarks/tera'"
            }
        }
        sleep(2000)
    }

    def deleteBenchmarkVolume(Map<String, ?> moduleconfig, role) {
        log.info(">>>>> Deleting /benchmarks volume.")
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume unmount -name benchmarks | xargs echo'"
                // xargs echo removes return code
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume remove -name benchmarks | xargs echo'"
                // xargs echo removes return code
                sleep(3000)
            }
        }
    }

    def generateTerasortBenchmark(Map<String, ?> moduleconfig, role, sparkTeraJar) {
        def result = [:]
        def dataSize = moduleconfig.getOrDefault("data_size", "1T")
        def numExecutors = moduleconfig.getOrDefault("num_executors", 10)
        def executorCores = moduleconfig.getOrDefault("executor_cores", 2)
        def executorMemory = moduleconfig.getOrDefault("executor_memory", "4G")
        log.info(">>>>> Generate Data for Spark TeraSort: Data size: ${dataSize} - Number of executors: ${numExecutors} - Executor cores: ${executorCores} - Executor memory: ${executorMemory}")
        log.info(">>>>> ... be patient")
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def sparkPath = execute "ls -d /opt/mapr/spark/spark-2*"

                executeSudo """su - ${globalYamlConfig.mapr_user} -c '${sparkPath}/bin/spark-submit --master yarn --deploy-mode client \\
  --name "Spark TeraGen" \\
  --class com.github.ehiggs.spark.terasort.TeraGen \\
  --num-executors ${numExecutors} \\
  --executor-cores ${executorCores} \\
  --executor-memory ${executorMemory} \\
  ${sparkTeraJar} ${dataSize} /benchmarks/tera/in'
"""
                result = [
                        numExecutors: numExecutors,
                        executorCores: executorCores,
                        executorMemory: executorMemory,
                        dataSize: dataSize,
                        jobDurationInMs: (System.currentTimeMillis() - start)
                ]
            }
        }
        log.info(">>>>> ... Spark TeraGen finished")
        return result
    }

    def runTerasortBenchmark(Map<String, ?> moduleconfig, role, sparkTeraJar) {
        def result = [:]
        def dataSize = moduleconfig.getOrDefault("data_size", "1T")
        def numExecutors = moduleconfig.getOrDefault("num_executors", 10)
        def executorCores = moduleconfig.getOrDefault("executor_cores", 2)
        def executorMemory = moduleconfig.getOrDefault("executor_memory", "4G")
        log.info(">>>>> Running Spark TeraSort: Data size: ${dataSize} - Number of executors: ${numExecutors} - Executor cores: ${executorCores} - Executor memory: ${executorMemory}")
        log.info(">>>>> ... be patient")
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def sparkPath = execute "ls -d /opt/mapr/spark/spark-2*"

                executeSudo """su - ${globalYamlConfig.mapr_user} -c '${sparkPath}/bin/spark-submit --master yarn --deploy-mode client \\
  --name "Spark TeraSort" \\
  --class com.github.ehiggs.spark.terasort.TeraSort \\
  --num-executors ${numExecutors} \\
  --executor-cores ${executorCores} \\
  --executor-memory ${executorMemory} \\
      ${sparkTeraJar} /benchmarks/tera/in /benchmarks/tera/out'
"""
                result = [
                        numExecutors: numExecutors,
                        executorCores: executorCores,
                        executorMemory: executorMemory,
                        dataSize: dataSize,
                        jobDurationInMs: (System.currentTimeMillis() - start)
                ]
            }
        }
        log.info(">>>>> ... Spark TeraSort finished")
        return result
    }
}
