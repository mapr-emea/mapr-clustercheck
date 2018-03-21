package com.mapr.emea.ps.clustercheck.module.benchmark.yarn.terasort.mr

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
@ClusterCheckModule(name = "benchmark-yarn-terasort-mr", version = "1.0")
class BenchmarkYarnTerasortMrModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkYarnTerasortMrModule.class);

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
        return [role: "clusterjob-execution", tests: [["chunk_size_in_mb": 256, "rows": 10000000000, "rows_comment": "one row has 100 byte", reduce_tasks_per_node: 2, "topology": "/data", replication: 3, compression: "on"]]]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-mr'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        def numberOfNodes = globalYamlConfig.nodes.findAll { it.roles != null && it.roles.contains(role) }.size()
        if (numberOfNodes > 1) {
            throw new ModuleValidationException("Please specify a role for 'benchmark-yarn-terasort-mr'-module which exactly contains one node. Currently, there are ${numberOfNodes} nodes defined for role '${role}'.")
        }
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def hadoopPath = executeSudo("ls -d /opt/mapr/hadoop/hadoop-2*")
                def hadoopExamplesJar = executeSudo("ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar")
                if(hadoopExamplesJar.contains("No such file or directory")) {
                    throw new ModuleValidationException("Cannot find hadoop examples jar in path /opt/mapr/hadoop/hadoop-2*/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar")
                }
            }
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-mr'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        def tests = moduleconfig.tests
        def result = []
        for (def test  : tests) {
            def rows = test.getOrDefault("rows", 10000000000) as Long
            def topology = test.getOrDefault("topology", "/data")
            def replication = test.getOrDefault("replication", 1)
            def compression = test.getOrDefault("compression", "on")
            def chunkSize = test.getOrDefault("chunk_size_in_mb", 256) * 1024
            def reduceTasksPerNode = test.getOrDefault("reduce_tasks_per_node", 2)

            def settings = [reduceTasksPerNode: reduceTasksPerNode, chunkSize: chunkSize, compression: compression, topology: topology, replication: replication, rows: rows]
            def volumeName = "benchmarks_" + RandomStringUtils.random(8, true, true).toLowerCase()
            setupBenchmarkVolume(test, role, volumeName, settings)
            def teraGenResult = generateTerasortBenchmark(test, role, volumeName, settings)
            def teraSortResult = runTerasortBenchmark(test, role, volumeName, settings)
            result << [reduceTasksPerNode: reduceTasksPerNode, chunkSize: chunkSize, compression: compression, topology: topology, replication: replication, rows: rows, teraGen: teraGenResult, teraSort: teraSortResult]
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
>    Rows: ${result.rows},
>    Compression: ${result.compression},
>    Topology: ${result.topology},
>    Replication: ${result.replication}
>    Reduce tasks per node: ${result.reduceTasksPerNode}
>>> TeraSort:
>>>    Bytes written: ${result.teraGen.bytesWritten} bytes / ${result.teraGen.bytesWritten / 1024 / 1024 / 1024} GB
>>>    GC time elapsed: ${result.teraGen.gcTimeElapsedInMs} ms
>>>    CPU time spent: ${result.teraGen.cpuTimeSpentInMs} ms
>>>    Job duration: ${result.teraGen.jobDurationInMs} ms
>>> TeraSort:
>>>    Input split: ${result.teraSort.inputSplitBytes} bytes
>>>    Read: ${result.teraSort.bytesRead} bytes
>>>    Written: ${result.teraSort.bytesWritten} bytes
>>>    GC time elapsed: ${result.teraSort.gcTimeElapsedInMs} ms
>>>    CPU time spent: ${result.teraSort.cpuTimeSpentInMs} ms
>>>    Shuffled maps: ${result.teraSort.shuffledMaps} bytes
>>>    Launched map tasks: ${result.teraSort.launchedMapTasks}
>>>    Launched reduce tasks: ${result.teraSort.launchedReduceTasks} 
>>>    Data local map tasks: ${result.teraSort.dataLocalMapTasks}
>>>    Reduce shuffle: ${result.teraSort.reduceShuffleBytes} bytes

"""
        }
        return textReport
    }

    def setupBenchmarkVolume(Map<String, ?> moduleconfig, def role, def volumeName, final Map<String, ?> config) {
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
                executeSudo(suStr("maprcli volume remove -name ${volumeName} | xargs echo"))
                // xargs echo removes return code
            }
        }
    }

    def generateTerasortBenchmark(Map<String, ?> moduleconfig, role, volumeName, config) {
        log.info(">>>>> Generate Data for TeraSort: Rows: ${config.rows} - Data ${(config.rows * 100) / 1000 / 1000 / 1000} GB")
        log.info(">>>>> ... be patient")
        def result = [:]
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = executeSudo("ls -d /opt/mapr/hadoop/hadoop-2*")
                def hadoopExamplesJar = executeSudo("ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar")
                def teraGenOutout = executeSudo(suStr("""hadoop jar ${hadoopExamplesJar} teragen \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.disk=0 \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -Dmapreduce.map.output.compress=false \\
      ${config.rows} /${volumeName}/tera/in
"""))
                def tokens = teraGenOutout.tokenize('\n')
                result = [
                        bytesWritten:tokens.find{ it.contains("Bytes Written") }.tokenize('=')[1] as Long,
                        gcTimeElapsedInMs:tokens.find{ it.contains("GC time elapsed") }.tokenize('=')[1] as Long,
                        cpuTimeSpentInMs:tokens.find{ it.contains("CPU time spent") }.tokenize('=')[1] as Long,
                        jobDurationInMs: (System.currentTimeMillis() - start)
                ]
            }
        }
        log.info(">>>>> ... TeraGen finished")
        return result
    }

    def runTerasortBenchmark(Map<String, ?> moduleconfig, role, volumeName, config) {
        log.info(">>>>> Running TeraSort: Rows: ${config.rows} - Data ${(config.rows * 100) / 1000 / 1000 / 1000} GB")
        log.info(">>>>> ... be patient")
        def result = [:]
        def jsonSlurper = new JsonSlurper()
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = executeSudo("ls -d /opt/mapr/hadoop/hadoop-2*")
                def hadoopExamplesJar = executeSudo("ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar")
                def dashboardInfoJson = executeSudo(suStr("maprcli node list -columns hostname,cpus,service -json"))
                def dashboardConfig = jsonSlurper.parseText(dashboardInfoJson)
                def numberOfNodes = dashboardConfig.data.collect{ it.service.contains("nodemanager") }.size()
                def reduceTasks = config.reduceTasksPerNode * numberOfNodes
                def teraSortOutout = executeSudo(suStr("""hadoop jar ${hadoopExamplesJar} terasort \\
      -Dmapreduce.map.disk=0 \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.output.compress=false \\
      -Dmapreduce.map.sort.spill.percent=0.99 \\
      -Dmapreduce.reduce.disk=0 \\
      -Dmapreduce.reduce.cpu.vcores=0 \\
      -Dmapreduce.reduce.shuffle.parallelcopies=${ numberOfNodes } \\
      -Dmapreduce.reduce.merge.inmem.threshold=0 \\
      -Dmapreduce.task.io.sort.mb=480 \\
      -Dmapreduce.task.io.sort.factor=100 \\
      -Dmapreduce.job.reduces=${ reduceTasks } \\
      -Dmapreduce.job.reduce.slowstart.completedmaps=0.55 \\
      -Dyarn.app.mapreduce.am.log.level=ERROR \\
      /${volumeName}/tera/in /${volumeName}/tera/out
"""))

                def tokens = teraSortOutout.tokenize('\n')
                result = [
                    inputSplitBytes: tokens.find{ it.contains("Input split byte") }.tokenize('=')[1] as Long,
                    bytesRead: tokens.find{ it.contains("Bytes Read") }.tokenize('=')[1] as Long,
                    bytesWritten: tokens.find{ it.contains("Bytes Written") }.tokenize('=')[1] as Long,
                    gcTimeElapsedInMs: tokens.find{ it.contains("GC time elapsed") }.tokenize('=')[1] as Long,
                    cpuTimeSpentInMs: tokens.find{ it.contains("CPU time spent") }.tokenize('=')[1] as Long,
                    shuffledMaps: tokens.find{ it.contains("Shuffled Maps") }.tokenize('=')[1] as Long,
                    launchedMapTasks: tokens.find{ it.contains("Launched map tasks") }.tokenize('=')[1] as Long,
                    launchedReduceTasks: tokens.find{ it.contains("Launched reduce tasks") }.tokenize('=')[1] as Long,
                    dataLocalMapTasks: tokens.find{ it.contains("Data-local map tasks") }.tokenize('=')[1] as Long,
                    reduceShuffleBytes: tokens.find{ it.contains("Reduce shuffle bytes") }.tokenize('=')[1] as Long,
                    totalTimeSpentByAllMapsInOccupiedSplots: tokens.find{ it.contains("Total time spent by all maps in occupied slots") }.tokenize('=')[1] as Long,
                    totalTimeSpentByAllReducesInOccupiedSplots: tokens.find{ it.contains("Total time spent by all reduces in occupied slots") }.tokenize('=')[1] as Long,
                    totalTimeSpentByAllMapTasks: tokens.find{ it.contains("Total time spent by all map tasks") }.tokenize('=')[1] as Long,
                    totalMBsecondsTakenByAllMapTasks: tokens.find{ it.contains("Total megabyte-seconds taken by all map tasks") }.tokenize('=')[1] as Long,
                    totalMBsecondsTakenByAllReduceTasks: tokens.find{ it.contains("Total megabyte-seconds taken by all reduce tasks") }.tokenize('=')[1] as Long,
                    jobDurationInMs: (System.currentTimeMillis() - start)

                ]
            }
        }
        log.info(">>>>> ... TeraSort finished")
        return result
    }


    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket;${exec}'"
    }

}
