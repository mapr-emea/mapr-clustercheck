package com.mapr.emea.ps.clustercheck.module.benchmark.maprfs.dfsio

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
@ClusterCheckModule(name = "benchmark-maprfs-dfsio", version = "1.0")
class BenchmarkMaprFsDfsioModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkMaprFsDfsioModule.class);

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
        return [role: "clusterjob-execution", "dfsio_files_per_disk": 1, "dfsio_file_size_in_mb": 8196, "topology": "/data", replication: 1, compression: "on"]
    }

    @Override
    void validate() throws ModuleValidationException {
        def moduleconfig = globalYamlConfig.modules['benchmark-maprfs-dfsio'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        if (role == "all") {
            throw new ModuleValidationException("Please specify a role for 'benchmark-maprfs'-module which is not 'all'. Usually it should run only on one node.")
        }
        // TODO check for valid ticket, if secure cluster
        // TODO check that role has only one node inside
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-maprfs-dfsio'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        deleteBenchmarkVolume(moduleconfig, role)
        setupBenchmarkVolume(moduleconfig, role)
        def result = runDfsioBenchmark(moduleconfig, role)
        deleteBenchmarkVolume(moduleconfig, role)
        return new ClusterCheckResult(reportJson: result, reportText: generateTextReport(result), recommendations: [])
    }

    def generateTextReport(results) {
        def textReport = ""
        for (def result : results) {
            textReport += """Executed on host: ${result.executedOnHost}
>>> Settings:
>>>    File size: ${result.fileSizeInMB} MB
>>>    Number of files: ${result.numberOfFiles},
>>>    Files per fisk: ${result.filesPerDisk},
>>>    Total disks: ${result.totalDisks},
>>>    Compression: ${result.compression},
>>> DFSIO write:
>>>    Number of files: ${result.write.numberOfFiles}
>>>    Total processed: ${result.write.totalProcessedInMB} MB
>>>    Throughput: ${result.write.throughputInMBperSecond} MB/second
>>>    Average IO rate: ${result.write.averageIORateInMBperSecond} MB/second
>>>    IO rate std deviation: ${result.write.ioRateStdDeviation}
>>>    Execution time: ${result.write.testExecTimeInSeconds} seconds
>>> DFSIO read:
>>>    Number of files: ${result.read.numberOfFiles}
>>>    Total processed: ${result.read.totalProcessedInMB} MB
>>>    Throughput: ${result.read.throughputInMBperSecond} MB/second
>>>    Average IO rate: ${result.read.averageIORateInMBperSecond} MB/second
>>>    IO rate std deviation: ${result.read.ioRateStdDeviation}
>>>    Execution time: ${result.read.testExecTimeInSeconds} seconds
"""
        }
        return textReport
    }

    def setupBenchmarkVolume(Map<String, ?> moduleconfig, role) {
        log.info(">>>>> Creating /benchmarks volume.")
        def topology = moduleconfig.getOrDefault("topology", "/data")
        def replication = moduleconfig.getOrDefault("replication", 1)
        def compression = moduleconfig.getOrDefault("compression", "on")

        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def topologyStr = topology != "/data" ? "-topology ${topology}" : ""
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume create -name benchmarks -path /benchmarks -replication ${replication} ${topologyStr}'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -chmod 777 /benchmarks'"
                executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop mfs -setcompression ${compression} /benchmarks'"
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

    def runDfsioBenchmark(Map<String, ?> moduleconfig, role) {
        def filesPerDisk = moduleconfig.getOrDefault("dfsio_files_per_disk", 1)
        def fileSizeInMB = moduleconfig.getOrDefault("dfsio_file_size_in_mb", 8196)
        def compression = moduleconfig.getOrDefault("compression", "on")
        def jsonSlurper = new JsonSlurper()
        log.info(">>>>> Run DFSIO tests... this can take some time.")
        def result = []
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = execute "ls -d /opt/mapr/hadoop/hadoop-2*"
                def testJar = execute "ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar"
                def dashboardJson = executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli dashboard info -json'"
//                new File("/Users/chufe/Documents/workspaces/mapr-clustercheck/playground/dashboard.json").text = dashboardJson
                def dashboardConfig = jsonSlurper.parseText(dashboardJson)
                def totalDisks = dashboardConfig.data[0].yarn.total_disks
                def mapDisk = 1 / filesPerDisk
                def numberOfFiles = totalDisks * filesPerDisk
                def dfsioWriteResult = executeSudo """su ${globalYamlConfig.mapr_user} -c 'hadoop jar ${testJar} TestDFSIO \\
      -Dmapreduce.job.name=mapr-clustercheck-DFSIO-write \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.memory.mb=768 \\
      -Dmapreduce.map.disk=${mapDisk} \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -write -nrFiles ${numberOfFiles} \\
      -fileSize ${fileSizeInMB}  -bufferSize 65536'
"""
                def dfsioReadResult = executeSudo """su ${globalYamlConfig.mapr_user} -c 'hadoop jar ${testJar} TestDFSIO \\
      -Dmapreduce.job.name=mapr-clustercheck-DFSIO-read \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.memory.mb=768 \\
      -Dmapreduce.map.disk=${mapDisk} \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -read -nrFiles ${numberOfFiles} \\
      -fileSize ${fileSizeInMB}  -bufferSize 65536'
"""
                def writeTokens = dfsioWriteResult.tokenize('\n')
                def readTokens = dfsioReadResult.tokenize('\n')
                result << [
                        executedOnHost: remote.host,
                        fileSizeInMB: fileSizeInMB,
                        numberOfFiles: numberOfFiles,
                        filesPerDisk: filesPerDisk,
                        totalDisks: totalDisks,
                        compression: compression,
                        write         : [
                                numberOfFiles             : getDoubleValueFromTokens(writeTokens, "Number of files"),
                                totalProcessedInMB        : getDoubleValueFromTokens(writeTokens, "Total MBytes processed"),
                                throughputInMBperSecond   : getDoubleValueFromTokens(writeTokens, "Throughput mb/sec"),
                                averageIORateInMBperSecond: getDoubleValueFromTokens(writeTokens, "Average IO rate mb/sec"),
                                ioRateStdDeviation        : getDoubleValueFromTokens(writeTokens, "IO rate std deviation"),
                                testExecTimeInSeconds     : getDoubleValueFromTokens(writeTokens, "Test exec time sec")
                        ],
                        read          : [
                                numberOfFiles             : getDoubleValueFromTokens(readTokens, "Number of files"),
                                totalProcessedInMB        : getDoubleValueFromTokens(readTokens, "Total MBytes processed"),
                                throughputInMBperSecond   : getDoubleValueFromTokens(readTokens, "Throughput mb/sec"),
                                averageIORateInMBperSecond: getDoubleValueFromTokens(readTokens, "Average IO rate mb/sec"),
                                ioRateStdDeviation        : getDoubleValueFromTokens(readTokens, "IO rate std deviation"),
                                testExecTimeInSeconds     : getDoubleValueFromTokens(readTokens, "Test exec time sec")
                        ]
                ]
            }
        }
    }


    def getDoubleValueFromTokens(tokens, description) {
        def line = tokens.find { it.contains(description) }
        return Double.valueOf(line.tokenize(" ")[-1])
    }
}
