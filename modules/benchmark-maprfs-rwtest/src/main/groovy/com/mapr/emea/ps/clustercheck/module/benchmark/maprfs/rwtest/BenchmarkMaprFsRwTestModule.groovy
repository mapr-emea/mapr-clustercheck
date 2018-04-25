package com.mapr.emea.ps.clustercheck.module.benchmark.maprfs.rwtest

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.apache.commons.lang.RandomStringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

import java.math.RoundingMode

/**
 * Created by chufe on 22.08.17.
 */
// TODO check the NIC speed and add to report
// TODO Give recommendation about speed
@ClusterCheckModule(name = "benchmark-maprfs-rwtest", version = "1.0")
class BenchmarkMaprFsRwTestModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkMaprFsRwTestModule.class);

    def defaultTestMatrix = [
            [volume_type: "standard", compression: "off", topology: "/data", size_in_mb: 16384, replication: 3, threads: 1],
            [volume_type: "standard", compression: "on", topology: "/data", size_in_mb: 16384, replication: 3, threads: 1],
            [volume_type: "local", compression: "off", use_disk_percentage: 5],
            [volume_type: "local", compression: "on", use_disk_percentage: 5]
    ]

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
        return [tests: defaultTestMatrix]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleConfig = globalYamlConfig.modules['benchmark-maprfs-rwtest'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        def standardVolumeResults = runStandardVolumeBenchmark(moduleConfig, role)
        def localVolumeResults = runLocalVolumeBenchmark(moduleConfig, role)
        def result = standardVolumeResults + localVolumeResults
        def textReport = buildTextReportForLocal(localVolumeResults) + buildTextReportForStandard(standardVolumeResults)
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: ["Verify that local write speed has around 80% of cumulated raw write speed."])
    }

    def buildTextReportForLocal(result) {
        def reportText = ""
        result.forEach { test ->
            def maxHostnameLength = getMaxLength(test.tests, "host")
            def text = "Local volume test - Compression: ${test.compression} - Use Disk: ${test.useDiskPercentage}% \n"
            text += "".padRight(text.size(), "-") + "\n"
            text += "Host".padRight(maxHostnameLength, " ") + "\tDisks\tData Size\tSum Write Speed\tSum Read Speed\n"

            for (def node in test.tests) {
                text += "${node['host']}\t${(node['numberOfDisks'] as String).padRight(5, " ")}\t${((node['dataSizeInMB'] as int) + " MB").padRight(9, " ")}\t${((node['sumWriteRateInMBperSecond'] as int) + " MB/s").padRight(15, " ")}\t${node['sumReadRateInMBperSecond'] as int} MB/s\n"
            }
            reportText += text + "\n"
        }
        return reportText
    }

    def buildTextReportForStandard(result) {
        def reportText = ""
        result.forEach { test ->
            def maxHostnameLength = getMaxLength(test.tests, "host")
            def text = "Standard volume test - Compression: ${test.compression} - Topology: ${test.topology}  - Replication: ${test.replication} - Data Size: ${test.dataSizeInMB} MB - Threads: ${test.threads}  \n"
            text += "".padRight(text.size(), "-") + "\n"
            text += "Host".padRight(maxHostnameLength, " ") + "\tData Size\tSum Write Speed\tSum Read Speed\n"

            for (def node in test.tests) {
                text += "${node['host']}\t${((test['dataSizeInMB'] as int) + " MB").padRight(9, " ")}\t${((node['sumWriteRateInMBperSecond'] as int) + " MB/s").padRight(15, " ")}\t${node['sumReadRateInMBperSecond'] as int} MB/s\n"
            }
            reportText += text + "\n"
        }
        return reportText
    }

    def getMaxLength(result, field) {
        def maxLength = 0
        for(def node in result) {
            if(maxLength < node[field].size()) {
                maxLength = node[field].size()
            }
        }
        return maxLength
    }


    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket;${exec}'"
    }

    def runLocalVolumeBenchmark(Map<String, ?> moduleconfig, role) {
        log.info("> Running local volume benchmark... be patient this can take a while.")
        def localConfigs = moduleconfig.tests.findAll { it.volume_type == "local" }
        def result = []
        localConfigs.each { localConfig ->
            def compression = localConfig.getOrDefault("compression", "off")
            def useDiskPercentage = localConfig.getOrDefault("use_disk_percentage", 5)
            def sizeString = localConfig.containsKey("size_in_mb") ? "Size: ${localConfig.size_in_mb} MB" : "Use free disk: ${useDiskPercentage}%"
            log.info(">>> Run test on local volume - ${sizeString} - Compression: ${compression}")
            def testCaseResults = []
            globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role) }.each { currentNode ->
         //   globalYamlConfig.nodes.each { currentNode ->
                def volumeName = "benchmarks_local_" + RandomStringUtils.random(8, true, true).toLowerCase()
                log.info(">>> ... on node ${currentNode.host} - Volume: ${volumeName}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(currentNode.host)) {
                        def diagnosticsJar = execute "ls /opt/mapr/lib/maprfs-diagnostic-tools-*.jar"
                        // def hostname = execute "hostname"
                        def hostnameFull = execute "hostname -f"
                        // def volumeName = "benchmarks_local_${hostname}_" + RandomStringUtils.random(8, true, true).toLowerCase()

                        def storagePoolOutput = executeSudo "su ${globalYamlConfig.mapr_user} -c '/opt/mapr/server/mrconfig sp list'"
                        def storagePoolFree = getTotalFreeInMB(storagePoolOutput)
                        def numberOfDisksOutput = executeSudo "su ${globalYamlConfig.mapr_user} -c \"/opt/mapr/server/mrconfig sp list -v | grep -o '/dev/[^ ,]*' | sort -u | wc -l\""
                        def numberOfDisks = numberOfDisksOutput as Integer
                        def sizeInMB = localConfig.getOrDefault("size_in_mb", (storagePoolFree * useDiskPercentage) / 100)
                        def dataSizePerThread = (sizeInMB / numberOfDisks) as Integer

                        // Create volume
                        executeSudo suStr("maprcli volume create -name ${volumeName} -path /${volumeName} -replication 1 -localvolumehost ${hostnameFull}")
                        executeSudo suStr("hadoop fs -chmod 777 /${volumeName}")
                        executeSudo suStr("hadoop mfs -setcompression ${compression} /${volumeName}")
                        // Run Write test
                    //    def homePath = executeSudo suStr("echo \$HOME")
                        def homePath = "/tmp"
                        def writeBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${numberOfDisks}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest\${i} ${dataSizePerThread} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                        executeSudo suStr("mkdir -p ${homePath}/.clustercheck")
                        put from: writeBashScript, into: "/tmp/rwtestread_local_write"
                        executeSudo suStr("cp /tmp/rwtestread_local_write ${homePath}/.clustercheck/rwtestread_local_write")
                        executeSudo suStr("chmod +x ${homePath}/.clustercheck/rwtestread_local_write")
                        def writeResult = executeSudo suStr("${homePath}/.clustercheck/rwtestread_local_write")


                        def readBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${numberOfDisks}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest\${i} ${dataSizePerThread * -1} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                        put from: readBashScript, into: "/tmp/rwtestread_local_read"
                        executeSudo suStr("cp /tmp/rwtestread_local_read ${homePath}/.clustercheck/rwtestread_local_read")
                        executeSudo suStr("chmod +x ${homePath}/.clustercheck/rwtestread_local_read")
                        def readResult = executeSudo suStr("${homePath}/.clustercheck/rwtestread_local_read")

                        // Delete volume
                        executeSudo suStr("maprcli volume unmount -name ${volumeName} | xargs echo")
// xargs echo removes return code
                        executeSudo suStr("maprcli volume remove -name ${volumeName} | xargs echo")
                        def writeRates = writeResult.tokenize('\n').findAll { it.startsWith("Write rate:") }.collect {
                            it.substring("Write rate:".size()).tokenize(' ')[0] as Double
                        }
                        def readRates = readResult.tokenize('\n').findAll { it.startsWith("Read rate:") }.collect {
                            it.substring("Read rate:".size()).tokenize(' ')[0] as Double
                        }

                        testCaseResults << [
                                host                     : remote.host,
                                numberOfDisks            : numberOfDisks,
                                dataSizeInMB             : sizeInMB,
                                writeRatesInMBperSecond  : writeRates,
                                readRatesInMBperSecond   : readRates,
                                sumWriteRateInMBperSecond: writeRates.sum(),
                                sumReadRateInMBperSecond : readRates.sum()
                        ]
                    }
                }
            }
            result << [
                    volumeType               : "local",
                    compression              : compression,
                    useDiskPercentage        : useDiskPercentage,
                    tests                    : testCaseResults
            ]
        }
        return result
    }

    def getTotalFreeInMB(String content) {
        def index = content.indexOf("totalfree")
        def tokens = content.substring(index).tokenize(' ')
        return Long.valueOf(tokens[1])
    }

    def runStandardVolumeBenchmark(Map<String, ?> moduleconfig, role) {
        log.info("> Running standard volume benchmark... be patient this can take a while.")
        def standardConfigs = moduleconfig.tests.findAll { it.volume_type == "standard" }
        def result = []
        standardConfigs.each { standardConfig ->
            def topology = standardConfig.getOrDefault("topology", "/data")
            def replication = standardConfig.getOrDefault("replication", 3)
            def compression = standardConfig.getOrDefault("compression", "off")
            def sizeInMB = standardConfig.getOrDefault("size_in_mb", 8192)
            def threads = standardConfig.getOrDefault("threads", 4)
            def dataSizePerThread = (sizeInMB / threads) as Integer
            log.info(">>> Run test on standard volume - size: ${sizeInMB} MB - Threads: ${threads} - Topology: ${topology} - Replication: ${replication} - Compression: ${compression}")
            def testCaseResults = []
            globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role) }.each { currentNode ->
                def volumeName = "benchmarks_" + RandomStringUtils.random(8, true, true).toLowerCase()
                log.info(">>> ... on node ${currentNode.host} - Volume: ${volumeName}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(currentNode.host)) {
                        def topologyStr = topology != "/data" ? "-topology ${topology}" : ""
                        def diagnosticsJar = execute "ls /opt/mapr/lib/maprfs-diagnostic-tools-*.jar"
                        // Create volume
                        executeSudo suStr("maprcli volume create -name ${volumeName} -path /${volumeName} -replication ${replication} ${topologyStr}")
                        sleep(3000)
                        executeSudo suStr("hadoop fs -chmod 777 /${volumeName}")
                        executeSudo suStr("hadoop mfs -setcompression ${compression} /${volumeName}")

                        // Run Write test
                        def homePath = "/tmp"
                    //    def homePath = executeSudo suStr("echo \$HOME")
                        def writeBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${threads}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest\${i} ${dataSizePerThread} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                        executeSudo suStr("mkdir -p ${homePath}/.clustercheck")
                        put from: writeBashScript, into: "/tmp/rwtestwrite_standard_write"
                        executeSudo suStr("cp /tmp/rwtestwrite_standard_write ${homePath}/.clustercheck/rwtestwrite_standard_write")
                        executeSudo suStr("chmod +x ${homePath}/.clustercheck/rwtestwrite_standard_write")
                        def writeResult = executeSudo suStr("${homePath}/.clustercheck/rwtestwrite_standard_write")


                        def readBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${threads}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest\${i} ${dataSizePerThread * -1} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                        put from: readBashScript, into: "/tmp/rwtestread_standard_read"
                        executeSudo suStr("cp /tmp/rwtestread_standard_read ${homePath}/.clustercheck/rwtestread_standard_read")
                        executeSudo suStr("chmod +x ${homePath}/.clustercheck/rwtestread_standard_read")
                        def readResult = executeSudo suStr("${homePath}/.clustercheck/rwtestread_standard_read")
                        // Delete volume
                        executeSudo suStr("maprcli volume unmount -name ${volumeName} | xargs echo"
                        )// xargs echo removes return code
                        executeSudo suStr("maprcli volume remove -name ${volumeName} | xargs echo")
                        def writeRates = writeResult.tokenize('\n').findAll { it.startsWith("Write rate:") }.collect {
                            it.substring("Write rate:".size()).tokenize(' ')[0] as Double
                        }
                        def readRates = readResult.tokenize('\n').findAll { it.startsWith("Read rate:") }.collect {
                            it.substring("Read rate:".size()).tokenize(' ')[0] as Double
                        }
                        testCaseResults << [
                                   host                     : remote.host,
                                   writeRatesInMBperSecond  : writeRates,
                                   readRatesInMBperSecond   : readRates,
                                   sumWriteRateInMBperSecond: writeRates.sum(),
                                   sumReadRateInMBperSecond : readRates.sum()
                        ]
                    }
                }
            }
            result << [
                    volumeType               : "standard",
                    topology                 : topology,
                    replication              : replication,
                    compression              : compression,
                    dataSizeInMB             : sizeInMB as Long,
                    threads                  : threads as Long,
                    tests                    : testCaseResults
            ]
        }
        return result
    }

}
