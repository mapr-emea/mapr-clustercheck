package com.mapr.emea.ps.clustercheck.module.benchmark.yarn.terasort.mr

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
    void validate() throws ModuleValidationException {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-mr'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        if (role == "all") {
            throw new ModuleValidationException("Please specify a role for 'benchmark-maprfs'-module which is not 'all'. Usually it should run only on one node.")
        }
        // TODO check for valid ticket, if secure cluster
        // TODO check that role has only one node inside
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-yarn-terasort-mr'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        deleteBenchmarkVolume(moduleconfig, role)
        def tests = moduleconfig.tests
        def result = []
        for (def test  : tests) {
            setupBenchmarkVolume(test, role)
            def teraGenResult = generateTerasortBenchmark(test, role)
            def teraSortResult = runTerasortBenchmark(test, role)
            result << [teraGen: teraGenResult, teraSort: teraSortResult]
            deleteBenchmarkVolume(test, role)
        }
        return new ClusterCheckResult(reportJson: result, reportText: "not yet implemented", recommendations: [])
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

    def generateTerasortBenchmark(Map<String, ?> moduleconfig, role) {
        def rows = moduleconfig.getOrDefault("rows", 10000000000) as Long
        log.info(">>>>> Generate Data for TeraSort: Rows: ${rows} - Data ${(rows * 100) / 1000 / 1000 / 1000} GB")
        log.info(">>>>> ... be patient")
        def result = [:]
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = execute "ls -d /opt/mapr/hadoop/hadoop-2*"
                def hadoopExamplesJar = execute "ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar"


                def teraGenOutout = executeSudo """su - ${globalYamlConfig.mapr_user} -c 'hadoop jar ${hadoopExamplesJar} teragen \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.disk=0 \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -Dmapreduce.map.output.compress=false \\
      ${rows} /benchmarks/tera/in'
"""
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

    def runTerasortBenchmark(Map<String, ?> moduleconfig, role) {
        def rows = moduleconfig.getOrDefault("rows", 10000000000) as Long
        def reduceTasksPerNode = moduleconfig.getOrDefault("reduce_tasks_per_node", 2)
        log.info(">>>>> Running TeraSort: Rows: ${rows} - Data ${(rows * 100) / 1000 / 1000 / 1000} GB")
        log.info(">>>>> ... be patient")
        def result = [:]
        def jsonSlurper = new JsonSlurper()
        def start = System.currentTimeMillis()
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = execute "ls -d /opt/mapr/hadoop/hadoop-2*"
                def hadoopExamplesJar = execute "ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar"
                def dashboardInfoJson = executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli node list -columns hostname,cpus,service -json'"
                def dashboardConfig = jsonSlurper.parseText(dashboardInfoJson)
                def numberOfNodes = dashboardConfig.data.collect{ it.service.contains("nodemanager") }.size()
                def reduceTasks = reduceTasksPerNode * numberOfNodes
       //         executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -rm -r /benchmarks/tera/out' | xargs echo"
                def teraSortOutout = executeSudo """su - ${globalYamlConfig.mapr_user} -c 'hadoop jar ${hadoopExamplesJar} terasort \\
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
      /benchmarks/tera/in /benchmarks/tera/out'
"""

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
}
