package com.mapr.emea.ps.clustercheck.module.benchmark.rawdisk

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.apache.commons.io.FilenameUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "benchmark-rawdisk", version = "1.0")
class BenchmarkRawDiskModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkRawDiskModule.class);

    @Autowired
    @Qualifier("ssh")
    def ssh
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    ResourceLoader resourceLoader;

    def defaultTestMatrix = [
            [mode: "READONLY", data_in_mb: 4096]
    ]

    @Override
    Map<String, ?> yamlModuleProperties() {
        return [tests: defaultTestMatrix, disks: ["/dev/sdb", "/dev/sdc", "/dev/sdd"]]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>
        moduleConfig.tests.each {
            if(it.mode != "READONLY" && it.mode != "DESTROY") {
                throw new ModuleValidationException("Only mode 'READONLY' and 'DESTROY' is allowed.")
            }
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        def result = runReadOnlyTests(role)
        result += runDestroyTests(role)

        def textReport = "" // buildTextReport(result)
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: getRecommendations())
    }

    def getRecommendations() {
        return ["Check that disks have expected speed.", "Check that disk speed is similar."]
    }

    def runDestroyTests(role) {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>

        def destroyTests = moduleConfig.tests.findAll { it.mode == "DESTROY" }
        if(destroyTests.size() > 0) {
            copyToolToRemoteHost(role, "iozone")
        }
        def nodes = globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role)  }
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running DESTROY disks tests")
        log.info(">>>>> ... this can take some time.")
        destroyTests.each { readOnlyTest ->
            def dataInMB = readOnlyTest.getOrDefault("data_in_mb", 4)
            def tests = []
            log.info(">>>>> ... test with ${dataInMB} MB")
            nodes.forEach { currentNode ->
                log.info(">>>>> ...... on node ${currentNode.host}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(role)) {
                        def homePath = execute 'echo $HOME'

                        def node = [:]
                        def wardenStatus = executeSudo("service mapr-warden status > /dev/null 2>&1; echo \$?")
                        node['host'] = remote.host
                        if(wardenStatus.trim() != "4") {
                            node['error'] = "Node has a mapr-warden service available. For safety reasons, destructive tests will not run on nodes with MapR's Warden being installed."
                            tests.add(node)
                            return
                        }
                        def iozoneStatus = executeSudo("pgrep iozone; echo \\\$?")
                        if(iozoneStatus.trim() == "0") {
                            node['error'] = "Seems that IOzone is still running."
                            tests.add(node)
                            return
                        }
                        def disks = currentNode.getOrDefault('disks', globalYamlConfig['nodes-global-config']['disks'])
                        def bashScript = new ByteArrayInputStream("""#!/bin/bash
disklist="${disks.join(' ')}"
size=${dataInMB}

iozopts="-I -r 1M -s \${size}M -k 10 -+n -i 0 -i 1 -i 2"
for disk in \$disklist; do
  iozlog=${homePath}/.clustercheck/\$(basename \$disk)-iozone.log
  ${homePath}/.clustercheck/iozone \$iozopts -f \$disk > \$iozlog &
  sleep 2 #Some disk controllers lockup without a delay
done
wait
""".getBytes())


                        executeSudo("mkdir -p ${homePath}/.clustercheck")
                        put from: bashScript, into: "/tmp/benchmark-rawdisk-destroy"
                        executeSudo("cp /tmp/benchmark-rawdisk-destroy ${homePath}/.clustercheck/benchmark-rawdisk-destroy")
                        executeSudo("chmod +x ${homePath}/.clustercheck/benchmark-rawdisk-destroy")
                        sleep(1000)

                        def readResult = executeSudo("${homePath}/.clustercheck/benchmark-rawdisk-destroy")
                        if(readResult.contains("Invalid argument")) {
                            node['error'] = "Cannot read disk: " + readResult
                            tests.add(node)
                            return
                        }
                        def diskTests = disks.collect { disk ->
                            def diskBasename = FilenameUtils.getBaseName(disk)
                            def content = executeSudo("cat ${homePath}/.clustercheck/${diskBasename}-iozone.log")
                            def tokens = content.tokenize('\n').find { it =~ /^[\d ]*$/ }.trim().tokenize().collect{ it as int }
                            return [disk: disk,
                                    dataInKB: tokens[0],
                                    reclen: tokens[1],
                                    seqWriteInKBperSecond: tokens[2],
                                    seqReadInKBperSecond: tokens[4],
                                    randomReadInKBperSecond: tokens[6],
                                    randomWriteInKBperSecond: tokens[7]
                            ]
                        }
                        node['minDiskSeqWriteInKBperSecond'] = diskTests.collect{ it['seqWriteInKBperSecond'] }.min()
                        node['minDiskSeqReadInKBperSecond'] = diskTests.collect{ it['seqReadInKBperSecond'] }.min()
                        node['minDiskRandomReadInKBperSecond'] = diskTests.collect{ it['randomReadInKBperSecond'] }.min()
                        node['minDiskRandomWriteInKBperSecond'] = diskTests.collect{ it['randomWriteInKBperSecond'] }.min()

                        node['maxDiskSeqWriteInKBperSecond'] = diskTests.collect{ it['seqWriteInKBperSecond'] }.max()
                        node['maxDiskSeqReadInKBperSecond'] = diskTests.collect{ it['seqReadInKBperSecond'] }.max()
                        node['maxDiskRandomReadInKBperSecond'] = diskTests.collect{ it['randomReadInKBperSecond'] }.max()
                        node['maxDiskRandomWriteInKBperSecond'] = diskTests.collect{ it['randomWriteInKBperSecond'] }.max()

                        node['sumDiskSeqWriteInKBperSecond'] = diskTests.sum{ it['seqWriteInKBperSecond'] }
                        node['sumDiskSeqReadInKBperSecond'] = diskTests.sum{ it['seqReadInKBperSecond'] }
                        node['sumDiskRandomReadInKBperSecond'] = diskTests.sum{ it['randomReadInKBperSecond'] }
                        node['sumDiskRandomWriteInKBperSecond'] = diskTests.sum{ it['randomWriteInKBperSecond'] }

                        node['meanDiskSeqWriteInKBperSecond'] = (node['sumDiskSeqWriteInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskSeqReadInKBperSecond'] = (node['sumDiskSeqReadInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskRandomReadInKBperSecond'] = (node['sumDiskRandomReadInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskRandomWriteInKBperSecond'] =  (node['sumDiskRandomWriteInKBperSecond'] / diskTests.size()) as int
                        node['diskTests'] = diskTests
                        tests.add(node)
                    }
                }
            }
            def testResult = [
                    dataInMB: dataInMB,
                    mode: "DESTROY",
                    sumNodeThroughputInMBperSecond: tests.sum{ it['sumDiskSeqWriteInKBperSecond'] },
                    sumNodeSeqReadInKBperSecond: tests.sum{ it['sumDiskSeqReadInKBperSecond'] },
                    sumNodeRandomReadInKBperSecond: tests.sum{ it['sumDiskRandomReadInKBperSecond'] },
                    sumNodeRandomWriteInKBperSecond: tests.sum{ it['sumDiskRandomWriteInKBperSecond'] },
                    meanNodeThroughputInMBperSecond: (tests.sum{ it['sumDiskSeqWriteInKBperSecond'] } / tests.size()) as int,
                    meanNodeSeqReadInKBperSecond: (tests.sum{ it['sumDiskSeqReadInKBperSecond'] } / tests.size()) as int ,
                    meanNodeRandomReadInKBperSecond: (tests.sum{ it['sumDiskRandomReadInKBperSecond'] } / tests.size()) as int ,
                    meanNodeRandomWriteInKBperSecond: (tests.sum{ it['sumDiskRandomWriteInKBperSecond'] } / tests.size()) as int ,
                    minNodeSeqWriteInKBperSecond: tests.collect{ it['sumDiskSeqWriteInKBperSecond'] }.min(),
                    minNodeSeqReadInKBperSecond: tests.collect{ it['sumDiskSeqReadInKBperSecond'] }.min(),
                    minNodeRandomReadInKBperSecond: tests.collect{ it['sumDiskRandomReadInKBperSecond'] }.min(),
                    minNodeRandomWriteInKBperSecond: tests.collect{ it['sumDiskRandomWriteInKBperSecond'] }.min(),
                    maxNodeSeqWriteInKBperSecond: tests.collect{ it['sumDiskSeqWriteInKBperSecond'] }.max(),
                    maxNodeSeqReadInKBperSecond: tests.collect{ it['sumDiskSeqReadInKBperSecond'] }.max(),
                    maxNodeRandomReadInKBperSecond: tests.collect{ it['sumDiskRandomReadInKBperSecond'] }.max(),
                    maxNodeRandomWriteInKBperSecond: tests.collect{ it['sumDiskRandomWriteInKBperSecond'] }.max(),
                    tests: tests]
            result.add(testResult)
        }
        log.info(">>>>> DESTROY disks tests finished")
        return result
    }

    def runReadOnlyTests(role) {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>
        def readOnlyTests = moduleConfig.tests.findAll { it.mode == "READONLY" }
        def nodes = globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role)  }
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running READONLY disks tests")
        log.info(">>>>> ... this can take some time.")
        readOnlyTests.each { readOnlyTest ->
            def dataInMB = readOnlyTest.getOrDefault("data_in_mb", 4)
            def tests = []
            log.info(">>>>> ... test with ${dataInMB} MB")
            nodes.forEach { currentNode ->
                log.info(">>>>> ...... on node ${currentNode.host}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(role)) {
                        def homePath = execute 'echo $HOME'

                        def node = [:]
                        def disks = currentNode.getOrDefault('disks', globalYamlConfig['nodes-global-config']['disks'])
                        def bashScript = new ByteArrayInputStream("""#!/bin/bash
disklist="${disks.join(' ')}"
size=${dataInMB}

ddopts="of=/dev/null iflag=direct bs=1M count=\$size"
for i in \$disklist; do
#    dd if=\$i \$ddopts |& tee \$(basename \$i)-dd.log &
    dd if=\$i \$ddopts |& tee ${homePath}/.clustercheck/\$(basename \$i)-dd.log &
done
wait
sleep 3
for i in \$disklist; do echo "\$i "; grep 'MB/s' ${homePath}/.clustercheck/\$(basename \$i)-dd.log; done
""".getBytes())


                        execute("mkdir -p ${homePath}/.clustercheck")
                        put from: bashScript, into: "/tmp/benchmark-rawdisk-readonly"
                        execute("cp /tmp/benchmark-rawdisk-readonly ${homePath}/.clustercheck/benchmark-rawdisk-readonly")
                        execute("chmod +x ${homePath}/.clustercheck/benchmark-rawdisk-readonly")
                        sleep(1000)
                        def readResult = executeSudo("${homePath}/.clustercheck/benchmark-rawdisk-readonly")
                        if(readResult.contains("failed to open")) {
                            node['host'] = remote.host
                            node['error'] = readResult
                            tests.add(node)
                            return
                        }
                        def lines = readResult.tokenize('\n')
                        def diskTests = disks.collect { disk ->
                            def dataIdx = lines.findIndexOf { it.trim() == disk.trim() } + 1
                            def res = [:]
                            res['disk'] = disk
                            def data = lines[dataIdx].tokenize(',')
                            res['readBytes'] = data[0].trim().tokenize(' ')[0] as Long
                            res['timeInSeconds'] = data[1].trim().tokenize(' ')[0] as Double
                            res['throughputInMBperSecond'] = data[2].trim().tokenize(' ')[0] as Double
                            return res
                        }
                        node['host'] = remote.host
                        node['minDiskThroughputInMBperSecond'] = diskTests.collect{ it['throughputInMBperSecond'] }.min()
                        node['maxDiskThroughputInMBperSecond'] = diskTests.collect{ it['throughputInMBperSecond'] }.max()
                        node['sumDiskThroughputInMBperSecond'] = diskTests.sum{ it['throughputInMBperSecond'] }
                        node['meanDiskThroughputInMBperSecond'] = (node['sumDiskThroughputInMBperSecond'] / diskTests.size())
                        node['diskTests'] = diskTests

                        tests.add(node)
                    }
                }
            }

            def testHost = [dataInMB: dataInMB,
                            mode: "READONLY",
                            minHostThroughputInMBperSecond: tests.collect{ it['minDiskThroughputInMBperSecond'] }.min(),
                            maxHostThroughputInMBperSecond: tests.collect{ it['maxDiskThroughputInMBperSecond'] }.max(),
                            sumHostThroughputInMBperSecond: tests.sum{ it['sumDiskThroughputInMBperSecond'] },
                            meanHostThroughputInMBperSecond: (tests.sum{ it['meanDiskThroughputInMBperSecond'] } / tests.size()),
                            tests: tests]
            result.add(testHost)
        }
        log.info(">>>>> READONLY disks tests finished")
        return result
    }

    def copyToolToRemoteHost(role, tool) {
        log.info(">>>>> Copy ${tool} to remote hosts")
        ssh.run {
            session(ssh.remotes.role(role)) {
                def homePath = execute 'echo $HOME'
                execute "mkdir -p ${homePath}/.clustercheck"
                def toolInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/rawdisk/" + tool).getInputStream()
                put from: toolInputStream, into: "${homePath}/.clustercheck/" + tool
                execute "chmod +x ${homePath}/.clustercheck/" + tool
            }
        }
    }

    def getColonValue(String line, String property) {
        return line.substring(line.indexOf(property) + property.length()).trim()
    }

    def getColonValueFromLines(String allLines, String property) {
        def line = findLine(allLines, property)
        if (!line) {
            return ""
        }
        return getColonValue(line, property)
    }

    def findLine(String allLines, String property) {
        def tokens = allLines.tokenize('\n')
        def result = tokens.find { it.trim().contains(property) }
        return result
    }
}
