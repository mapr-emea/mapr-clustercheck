package com.mapr.emea.ps.clustercheck.module.benchmark.yarn.terasort.spark

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import groovy.json.JsonSlurper
import org.apache.commons.lang.RandomStringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

/**
 * Created by chufe on 22.08.17.
 */
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
        def numberOfNodes = globalYamlConfig.nodes.findAll { role == "all" || (it.roles != null && it.roles.contains(role)) }.size()
        if (numberOfNodes != 1) {
            throw new ModuleValidationException("Please specify a role for 'benchmark-yarn-terasort-spark'-module which exactly contains one node. Currently, there are ${numberOfNodes} nodes defined for role '${role}'.")
        }
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def sparkSubmitPath = executeSudo("ls /opt/mapr/spark/spark-2*/bin/spark-submit")
                if(sparkSubmitPath.contains("No such file or directory")) {
                    throw new ModuleValidationException("Cannot find an installed Spark 2.x version with spark-submit in path /opt/mapr/spark/spark-2*/bin/spark-submit")
                }
            }
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-spark'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        def result = []
        def sparkTeraJar = copySparkTeraSortToRemoteHost(role)
        def tests = moduleconfig.tests
        for (def test  : tests) {
            def topology = test.getOrDefault("topology", "/data")
            def replication = test.getOrDefault("replication", 1)
            def compression = test.getOrDefault("compression", "on")
            def chunkSize = test.getOrDefault("chunk_size_in_mb", 256) * 1024
            def dataSize = test.getOrDefault("data_size", "1T")
            def numExecutors = test.getOrDefault("num_executors", 10)
            def executorCores = test.getOrDefault("executor_cores", 2)
            def executorMemory = test.getOrDefault("executor_memory", "4G")
            def config = [
                topology: topology,
                replication: replication,
                compression: compression,
                chunkSize: chunkSize,
                dataSize: dataSize,
                numExecutors: numExecutors,
                executorCores: executorCores,
                executorMemory: executorMemory
            ]
            def volumeName = "benchmarks_" + RandomStringUtils.random(8, true, true).toLowerCase()
            setupBenchmarkVolume(test, role, volumeName, config)
            def teraGenResult = generateTerasortBenchmark(test, role, sparkTeraJar, volumeName, config)
            def teraSortResult = runTerasortBenchmark(test, role, sparkTeraJar, volumeName, config)
            result << (config << [teraGen: teraGenResult, teraSort: teraSortResult])
            deleteBenchmarkVolume(test, role, volumeName)
        }
        def textReport = generateTextReport(result)
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: [])
    }

    def generateTextReport(results) {
        def textReport = ""
        for (def result : results) {
            textReport += """
> Test settings:
>    Chunk size: ${result.chunkSize} bytes
>    Compression: ${result.compression}
>    Topology: ${result.topology}
>    Replication: ${result.replication}
>    Data Size: ${result.dataSize}
>    Number of executors: ${result.numExecutors}
>    Executor cores: ${result.executorCores}
>    Executor memory: ${result.executorMemory}
>>> TeraSort:
>>>    Job duration: ${result.teraGen.jobDurationInMs} ms
>>> TeraSort:
>>>    Job duration: ${result.teraSort.jobDurationInMs} ms

"""
        }
        return textReport
    }

    def copySparkTeraSortToRemoteHost(role) {
        log.info(">>>>> Copy SparkTeraSort.jar to remote host")
        def sparkTeraJar = ""
        ssh.run {
            session(ssh.remotes.role(role)) {
                def inputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/yarn/terasort/spark/spark-2.1-terasort-1.1-SNAPSHOT.jar").getInputStream()
                put from: inputStream, into: "/tmp/spark-2.1-terasort-1.1-SNAPSHOT.jar"
                def homePath = executeSudo(suStr("echo \$HOME"))
                executeSudo(suStr("mkdir -p ${homePath}/.clustercheck"))
                executeSudo(suStr("cp /tmp/spark-2.1-terasort-1.1-SNAPSHOT.jar ${homePath}/.clustercheck/spark-2.1-terasort-1.1-SNAPSHOT.jar"))
                sparkTeraJar = "${homePath}/.clustercheck/spark-2.1-terasort-1.1-SNAPSHOT.jar"
            }
        }
        return sparkTeraJar
    }


    def setupBenchmarkVolume(Map<String, ?> moduleconfig, role, volumeName, config) {
        log.info(">>>>> Creating /${volumeName} volume.")
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def topologyStr = config.topology != "/data" ? "-topology ${config.topology}" : ""
                executeSudo(suStr("maprcli volume create -name ${volumeName} -path /${volumeName} -replication ${config.replication} ${topologyStr}"))
                executeSudo(suStr("hadoop fs -chmod 777 /${volumeName}"))
                executeSudo(suStr("hadoop fs -mkdir /${volumeName}/tera"))
                executeSudo(suStr("hadoop mfs -setcompression ${config.compression} /${volumeName}"))
                executeSudo(suStr("hadoop mfs -setchunksize ${config.chunkSize} /${volumeName}/tera"))
            }
        }
        sleep(2000)
    }

    def deleteBenchmarkVolume(Map<String, ?> moduleconfig, role, volumeName) {
        log.info(">>>>> Deleting /${volumeName} volume.")
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                executeSudo(suStr("maprcli volume unmount -name ${volumeName} | xargs echo"))
                // xargs echo removes return code
                executeSudo(suStr( "maprcli volume remove -name ${volumeName} | xargs echo"))
                // xargs echo removes return code
            }
        }
    }

    def generateTerasortBenchmark(Map<String, ?> moduleconfig, role, sparkTeraJar, volumeName, config) {
        def result = [:]
        log.info(">>>>> Generate Data for Spark TeraSort: Data size: ${config.dataSize} - Number of executors: ${config.numExecutors} - Executor cores: ${config.executorCores} - Executor memory: ${config.executorMemory}")
        log.info(">>>>> ... be patient")
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def sparkPath = execute "ls -d /opt/mapr/spark/spark-2*"
                executeSudo(suStr("""${sparkPath}/bin/spark-submit --master yarn --deploy-mode client \\
  --name "Spark TeraGen" \\
  --class com.github.ehiggs.spark.terasort.TeraGen \\
  --num-executors ${config.numExecutors} \\
  --executor-cores ${config.executorCores} \\
  --executor-memory ${config.executorMemory} \\
  ${sparkTeraJar} ${config.dataSize} /${volumeName}/tera/in
"""))
                result = [
                        jobDurationInMs: (System.currentTimeMillis() - start)
                ]
            }
        }
        log.info(">>>>> ... Spark TeraGen finished")
        return result
    }

    def runTerasortBenchmark(Map<String, ?> moduleconfig, role, sparkTeraJar, volumeName, config) {
        def result = [:]

        log.info(">>>>> Running Spark TeraSort: Data size: ${config.dataSize} - Number of executors: ${config.numExecutors} - Executor cores: ${config.executorCores} - Executor memory: ${config.executorMemory}")
        log.info(">>>>> ... be patient")
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def sparkPath = executeSudo "ls -d /opt/mapr/spark/spark-2*"

                executeSudo(suStr("""${sparkPath}/bin/spark-submit --master yarn --deploy-mode client \\
  --name "Spark TeraSort" \\
  --class com.github.ehiggs.spark.terasort.TeraSort \\
  --num-executors ${config.numExecutors} \\
  --executor-cores ${config.executorCores} \\
  --executor-memory ${config.executorMemory} \\
      ${sparkTeraJar} /${volumeName}/tera/in /${volumeName}/tera/out
"""))
                result = [
                        jobDurationInMs: (System.currentTimeMillis() - start)
                ]
            }
        }
        log.info(">>>>> ... Spark TeraSort finished")
        return result
    }

    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket;${exec}'"
    }
}
