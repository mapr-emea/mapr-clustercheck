package com.mapr.emea.ps.clustercheck.module.benchmark.maprfs.dfsio

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
        return [role: "clusterjob-execution", tests: [["dfsio_number_of_files": 1024, "dfsio_file_size_in_mb": 8196, "topology": "/data", replication: 1, compression: "on"]]]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        def moduleconfig = globalYamlConfig.modules['benchmark-maprfs-dfsio'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        def numberOfNodes = globalYamlConfig.nodes.findAll { it.roles != null && it.roles.contains(role) }.size()
        if (numberOfNodes > 1) {
            throw new ModuleValidationException("Please specify a role for 'benchmark-maprfs-dfsio'-module which exactly contains one node. Currently, there are ${numberOfNodes} nodes defined for role '${role}'.")
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleconfig = globalYamlConfig.modules['benchmark-maprfs-dfsio'] as Map<String, ?>
        def role = moduleconfig.getOrDefault("role", "all")
        def results = []
        def tests = moduleconfig.tests
        for (def test : tests) {
            // Some times it happens that reuse of volume name is not possible immedialtely
            def volumeName = "benchmarks_" + RandomStringUtils.random(8, true, true).toLowerCase()
            setupBenchmarkVolume(test, role, volumeName)
            results << runDfsioBenchmark(test, role, volumeName)
            deleteBenchmarkVolume(test, role, volumeName)
        }
        return new ClusterCheckResult(reportJson: results, reportText: generateTextReport(results), recommendations: [])
    }

    def generateTextReport(results) {
        def textReport = ""
        for (def result : results) {
            textReport += """
> Test settings:
>    File size: ${result.fileSizeInMB} MB
>    Files per fisk: ${result.numberOfFiles},
>    Compression: ${result.compression},
>    Topology: ${result.topology},
>    Replication: ${result.replication}
"""
            for (def test : result.tests) {
                textReport += """>>> Host settings:         
>>>    Executed on: ${test.executedOnHost}
>>>    Number of files: ${test.numberOfFiles}
>>> DFSIO write:
>>>    Number of files: ${test.write.numberOfFiles}
>>>    Total processed: ${test.write.totalProcessedInMB} MB
>>>    Throughput: ${test.write.throughputInMBperSecond} MB/second
>>>    Average IO rate: ${test.write.averageIORateInMBperSecond} MB/second
>>>    IO rate std deviation: ${test.write.ioRateStdDeviation}
>>>    Execution time: ${test.write.testExecTimeInSeconds} seconds
>>> DFSIO read:
>>>    Number of files: ${test.read.numberOfFiles}
>>>    Total processed: ${test.read.totalProcessedInMB} MB
>>>    Throughput: ${test.read.throughputInMBperSecond} MB/second
>>>    Average IO rate: ${test.read.averageIORateInMBperSecond} MB/second
>>>    IO rate std deviation: ${test.read.ioRateStdDeviation}
>>>    Execution time: ${test.read.testExecTimeInSeconds} seconds
"""
            }

        }
        return textReport
    }

    def setupBenchmarkVolume(Map<String, ?> moduleconfig, role, volumeName) {
        log.info(">>>>> Creating /${volumeName} volume.")
        def topology = moduleconfig.getOrDefault("topology", "/data")
        def replication = moduleconfig.getOrDefault("replication", 1)
        def compression = moduleconfig.getOrDefault("compression", "on")

        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def topologyStr = topology != "/data" ? "-topology ${topology}" : ""
                executeSudo suStr("maprcli volume create -name ${volumeName} -path /${volumeName} -replication ${replication} ${topologyStr}")
                executeSudo suStr("hadoop fs -chmod 777 /${volumeName}")
                executeSudo suStr("hadoop mfs -setcompression ${compression} /${volumeName}")
            }
        }
        sleep(3000)
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
                sleep(1000)
            }
        }
    }

    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket;${exec}'"
    }

    def runDfsioBenchmark(Map<String, ?> moduleconfig, role, volumeName) {
        def numberOfFiles = moduleconfig.getOrDefault("dfsio_number_of_files", 1024)
        def fileSizeInMB = moduleconfig.getOrDefault("dfsio_file_size_in_mb", 8196)
        def compression = moduleconfig.getOrDefault("compression", "on")
        def topology = moduleconfig.getOrDefault("topology", "/data")
        def replication = moduleconfig.getOrDefault("replication", 1)
//        def jsonSlurper = new JsonSlurper()
        log.info(">>>>> Run DFSIO tests - Files: ${numberOfFiles} - File size ${fileSizeInMB} MB - Compression: ${compression} - Topology: ${topology} - Replication: ${replication}")
        log.info(">>>>> ... this can take some time.")
        def result = []
        ssh.runInOrder {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {

                def hadoopPath = execute "ls -d /opt/mapr/hadoop/hadoop-2*"
                def testJar = execute "ls ${hadoopPath}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar"
//                def dashboardJson = executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli dashboard info -json'"
//                def dashboardConfig = jsonSlurper.parseText(dashboardJson)
//                def totalDisks = dashboardConfig.data[0].yarn.total_disks
//                def mapDisk = 1 / filesPerDisk
                def mapDisk = 1 / numberOfFiles
//                def numberOfFiles = totalDisks * filesPerDisk
                def startWrite = System.currentTimeMillis()
                def dfsioWriteResult = executeSudo suStr("""hadoop jar ${testJar} TestDFSIO \\
      -Dmapreduce.job.name=mapr-clustercheck-DFSIO-write \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.memory.mb=768 \\
      -Dmapreduce.map.disk=${mapDisk} \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -Dtest.build.data=/${volumeName}/TestDFSIO \\
      -write -nrFiles ${numberOfFiles} \\
      -fileSize ${fileSizeInMB}  -bufferSize 65536
""")
                def endWrite = System.currentTimeMillis()
                def startRead = System.currentTimeMillis()
                def dfsioReadResult = executeSudo suStr("""hadoop jar ${testJar} TestDFSIO \\
      -Dmapreduce.job.name=mapr-clustercheck-DFSIO-read \\
      -Dmapreduce.map.cpu.vcores=0 \\
      -Dmapreduce.map.memory.mb=768 \\
      -Dmapreduce.map.disk=${mapDisk} \\
      -Dmapreduce.map.speculative=false \\
      -Dmapreduce.reduce.speculative=false \\
      -Dtest.build.data=/${volumeName}/TestDFSIO \\
      -read -nrFiles ${numberOfFiles} \\
      -fileSize ${fileSizeInMB}  -bufferSize 65536
""")
                def endRead = System.currentTimeMillis()

                def writeTokens = dfsioWriteResult.tokenize('\n')
                def readTokens = dfsioReadResult.tokenize('\n')
                result << [
                        executedOnHost: remote.host,
                        numberOfFiles : numberOfFiles,
               //         totalDisks    : totalDisks,
                        write         : [
                                numberOfFiles             : getDoubleValueFromTokens(writeTokens, "Number of files"),
                                totalProcessedInMB        : getDoubleValueFromTokens(writeTokens, "Total MBytes processed"),
                                throughputInMBperSecond   : getDoubleValueFromTokens(writeTokens, "Throughput mb/sec"),
                                averageIORateInMBperSecond: getDoubleValueFromTokens(writeTokens, "Average IO rate mb/sec"),
                                ioRateStdDeviation        : getDoubleValueFromTokens(writeTokens, "IO rate std deviation"),
                                testExecTimeInSeconds     : getDoubleValueFromTokens(writeTokens, "Test exec time sec"),
                                durationInMs              : endWrite - startWrite
                        ],
                        read          : [
                                numberOfFiles             : getDoubleValueFromTokens(readTokens, "Number of files"),
                                totalProcessedInMB        : getDoubleValueFromTokens(readTokens, "Total MBytes processed"),
                                throughputInMBperSecond   : getDoubleValueFromTokens(readTokens, "Throughput mb/sec"),
                                averageIORateInMBperSecond: getDoubleValueFromTokens(readTokens, "Average IO rate mb/sec"),
                                ioRateStdDeviation        : getDoubleValueFromTokens(readTokens, "IO rate std deviation"),
                                testExecTimeInSeconds     : getDoubleValueFromTokens(readTokens, "Test exec time sec"),
                                durationInMs              : endRead - startRead
                        ]
                ]
            }
        }
        return [fileSizeInMB: fileSizeInMB,
                numberOfFiles: numberOfFiles,
                compression : compression,
                topology    : topology,
                replication : replication,
                tests       : result]
    }


    def getDoubleValueFromTokens(tokens, description) {
        def line = tokens.find { it.contains(description) }
        return Double.valueOf(line.tokenize(" ")[-1])
    }
}
