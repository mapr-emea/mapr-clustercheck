package com.mapr.emea.ps.clustercheck.module.benchmark.maprfs.rwtest

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

/**
 * Created by chufe on 22.08.17.
 */
// TODO check the NIC speed and add to report
// TODO Give recommendation about speed
@ClusterCheckModule(name = "benchmark-maprfs-rwtest", version = "1.0")
class BenchmarkMaprFsRwTestModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkMaprFsRwTestModule.class);

    def defaultTestMatrix = [
            [volume_type: "standard", compression: "off", topology: "/data", size_in_mb: 16384, replication: 3],
            [volume_type: "standard", compression: "on", topology: "/data", size_in_mb: 16384, replication: 3],
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
        return [tests:defaultTestMatrix]
    }

    @Override
    void validate() throws ModuleValidationException {
    }

    @Override
    ClusterCheckResult execute() {
        def moduleConfig = globalYamlConfig.modules['benchmark-maprfs-rwtest'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        def standardVolumeResults = runStandardVolumeBenchmark(moduleConfig, role)
        def localVolumeResults = runLocalVolumeBenchmark(moduleConfig, role)
        def result = standardVolumeResults + localVolumeResults
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
    }


    def runLocalVolumeBenchmark(Map<String, ?> moduleconfig, role) {
        log.info(">>>>> Running local volume benchmark... be patient this can take a while.")
        def localConfigs = moduleconfig.tests.findAll { it.volume_type == "local" }
        def result = []
        localConfigs.each { localConfig ->
            def compression = localConfig.getOrDefault("compression", "off")
            def useDiskPercentage = localConfig.getOrDefault("use_disk_percentage", 5)


            ssh.runInOrder {
                settings {
                    pty = true
                }
                session(ssh.remotes.role(role)) {
                    def diagnosticsJar =  execute "ls /opt/mapr/lib/maprfs-diagnostic-tools-*.jar"
                    def hostname =  execute "hostname"
                    def volumeName = "local1_clustercheck_${hostname}"
                    def storagePoolOutput = executeSudo "su ${globalYamlConfig.mapr_user} -c '/opt/mapr/server/mrconfig sp list'"
                    def storagePoolFree = getTotalFreeInMB(storagePoolOutput)
                    def numberOfDisksOutput = executeSudo "su ${globalYamlConfig.mapr_user} -c \"/opt/mapr/server/mrconfig sp list -v | grep -o '/dev/[^ ,]*' | sort -u | wc -l\""
                    def numberOfDisks = numberOfDisksOutput as Integer
                    def sizeInMB = localConfig.getOrDefault("size_in_mb", (storagePoolFree * useDiskPercentage) / 100)
                    def dataSizePerThread = (sizeInMB / numberOfDisks) as Integer

                    // Delete volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume unmount -name ${volumeName} | xargs echo'"// xargs echo removes return code
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume remove -name ${volumeName} | xargs echo'"
                    // Create volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume create -name ${volumeName} -path /${volumeName} -replication 1 -localvolumehost ${hostname}'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -chmod 777 /${volumeName}'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop mfs -setcompression ${compression} /${volumeName}'"
                    // Run Write test
                    //def writeResult = executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest ${sizeInMB} maprfs:///'"
//                    def writeResult = executeSudo "su ${globalYamlConfig.mapr_user} -c 'for i in \$(seq 1 ${numberOfDisks}); do (hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest ${dataSizePerThread} maprfs:/// &); done'; wait; sleep 3"
                    def homePath = executeSudo "su ${globalYamlConfig.mapr_user} -c 'echo \$HOME'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'mkdir -p ${homePath}/.clustercheck'"
                    def writeBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${numberOfDisks}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest ${dataSizePerThread} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                    put from: writeBashScript, into: "/tmp/rwtestwrite"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'cp /tmp/rwtestwrite ${homePath}/rwtestwrite'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'chmod +x ${homePath}/rwtestwrite'"
                    def writeResult = executeSudo "su ${globalYamlConfig.mapr_user} -c '${homePath}/rwtestwrite'"


                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'mkdir -p ${homePath}/.clustercheck'"
                    def readBashScript = new ByteArrayInputStream("""#!/usr/bin/env bash

for i in \$(seq 1 ${numberOfDisks}); do 
    hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /${volumeName}/RWTestSingleTest ${dataSizePerThread * -1} maprfs:/// & 
done 
wait 
sleep 3 
""".getBytes())

                    put from: readBashScript, into: "/tmp/rwtestread"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'cp /tmp/rwtestread ${homePath}/rwtestread'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'chmod +x ${homePath}/rwtestread'"
                    def readResult = executeSudo "su ${globalYamlConfig.mapr_user} -c '${homePath}//rwtestread'"

                    // Delete volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume unmount -name ${volumeName} | xargs echo'"// xargs echo removes return code
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume remove -name ${volumeName} | xargs echo'"
                    sleep(2000)
                    def writeRates = writeResult.tokenize('\n').findAll{ it.startsWith("Write rate")}.collect{ Double.valueOf(it.tokenize(' ')[-2]) }
                    def readRates = readResult.tokenize('\n').findAll{ it.startsWith("Read rate")}.collect{ Double.valueOf(it.tokenize(' ')[-2]) }
                    result << [
                            host: remote.host,
                            volumeType: "local",
                            compression: compression,
                            useDiskPercentage: useDiskPercentage,
                            dataSizeInMB: sizeInMB,
                            numberOfDisks: numberOfDisks,
                            writeRatesInMBperSecond: writeRates,
                            readRatesInMBperSecond: readRates,
                            sumWriteRateInMBperSecond: writeRates.sum(),
                            sumReadRateInMBperSecond: readRates.sum()
                    ]

                }
            }
        }
        return result
    }

    def getTotalFreeInMB(String content) {
        def index = content.indexOf("totalfree")
        def tokens = content.substring(index).tokenize(' ')
        return Long.valueOf(tokens[1])
    }

    def runStandardVolumeBenchmark(Map<String, ?> moduleconfig, role) {
        log.info(">>>>> Running standard volume benchmark... be patient this can take a while.")
        def standardConfigs = moduleconfig.tests.findAll { it.volume_type == "standard" }
        def result = []
        standardConfigs.each { standardConfig ->
            def topology = standardConfig.getOrDefault("topology", "/data")
            def replication = standardConfig.getOrDefault("replication", 3)
            def compression = standardConfig.getOrDefault("compression", "off")
            def sizeInMB = standardConfig.getOrDefault("size_in_mb", 8192)

            ssh.runInOrder {
                settings {
                    pty = true
                }
                session(ssh.remotes.role(role)) {
                    def topologyStr = topology != "/data" ? "-topology ${topology}" : ""
                    def diagnosticsJar =  execute "ls /opt/mapr/lib/maprfs-diagnostic-tools-*.jar"
                    // Delete volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume unmount -name benchmarks | xargs echo'"// xargs echo removes return code
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume remove -name benchmarks | xargs echo'"
                    // Create volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume create -name benchmarks -path /benchmarks -replication ${replication} ${topologyStr}'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop fs -chmod 777 /benchmarks'"
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop mfs -setcompression ${compression} /benchmarks'"
                    // Run Write test
                    def writeResult = executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /benchmark/RWTestSingleTest ${sizeInMB} maprfs:///'"
                    // Run Read test, read is enabled by negative value....
                    def readResult = executeSudo "su ${globalYamlConfig.mapr_user} -c 'hadoop jar ${diagnosticsJar} com.mapr.fs.RWSpeedTest /benchmark/RWTestSingleTest ${sizeInMB * -1} maprfs:///'"
                    //println getDoubleMegaBytesValueFromTokens(readResult)

                    // Delete volume
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume unmount -name benchmarks | xargs echo'"// xargs echo removes return code
                    executeSudo "su ${globalYamlConfig.mapr_user} -c 'maprcli volume remove -name benchmarks | xargs echo'"
                    sleep(2000)
                    result << [
                            host: remote.host,
                            volumeType: "standard",
                            topology: topology,
                            replication: replication,
                            compression: compression,
                            dataSizeInMB: sizeInMB,
                            writeRateInMBperSecond: getDoubleMegaBytesValueFromTokens(writeResult),
                            readRateInMBperSecond: getDoubleMegaBytesValueFromTokens(readResult),
                    ]
                }
            }
        }
        return result
    }

    def getDoubleMegaBytesValueFromTokens(content) {
        def tokens = content.tokenize(' ')
        return Double.valueOf(tokens[-2])
    }
}
