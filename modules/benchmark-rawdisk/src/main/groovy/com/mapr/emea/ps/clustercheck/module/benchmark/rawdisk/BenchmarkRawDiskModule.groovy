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
            if (it.mode != "READONLY" && it.mode != "DESTROY") {
                throw new ModuleValidationException("Only mode 'READONLY' and 'DESTROY' is allowed.")
            }
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        def readOnlyTests = runReadOnlyTests(role)
        def destroyTests = runDestroyTests(role)
        def textReport = buildTextReportReadOnlyTests(readOnlyTests)
        textReport += buildTextReportDestroyTests(destroyTests)
        return new ClusterCheckResult(reportJson: readOnlyTests + destroyTests, reportText: textReport, recommendations: getRecommendations())
    }

    def buildTextReportDestroyTests(tests) {
        def text = ""
        for (def test in tests) {
            def header = "Disk test - DESTROY with ${test.dataInMB} MB\n"
            text += header
            text += "".padRight(header.size() + 40, "-") + "\n\n"
            text += "Min host sequential write throughput: ${test.minNodeSeqWriteInKBperSecond} KB/s\n"
            text += "Min host sequential read throughput: ${test.minNodeSeqReadInKBperSecond} KB/s\n"
            text += "Min host random write throughput: ${test.minNodeRandomWriteInKBperSecond} KB/s\n"
            text += "Min host random read throughput: ${test.minNodeRandomReadInKBperSecond} KB/s\n"
            text += "Max host sequential write throughput: ${test.maxNodeSeqWriteInKBperSecond} KB/s\n"
            text += "Max host sequential read throughput: ${test.maxNodeSeqReadInKBperSecond} KB/s\n"
            text += "Max host random write throughput: ${test.maxNodeRandomWriteInKBperSecond} KB/s\n"
            text += "Max host ramdom read throughput: ${test.maxNodeRandomReadInKBperSecond} KB/s\n"
            text += "Mean host sequential write throughput: ${test.meanNodeSeqWriteInKBperSecond} KB/s\n"
            text += "Mean host sequential read throughput: ${test.meanNodeSeqReadInKBperSecond} KB/s\n"
            text += "Mean host random write throughput: ${test.meanNodeRandomWriteInKBperSecond} KB/s\n"
            text += "Mean host ramdom read throughput: ${test.meanNodeRandomReadInKBperSecond} KB/s\n"
            text += "Sum host sequential write throughput: ${test.sumNodeSeqWriteInKBperSecond} KB/s\n"
            text += "Sum host sequential read throughput: ${test.sumNodeSeqReadInKBperSecond} KB/s\n"
            text += "Sum host random write throughput: ${test.sumNodeRandomWriteInKBperSecond} KB/s\n"
            text += "Sum host ramdom read throughput: ${test.sumNodeRandomReadInKBperSecond} KB/s\n"
            text += buildDestroyTestNodeStats(test.hostTests)
        }
        text
    }

    def buildDestroyTestNodeStats(tests) {
        def maxHostLength = getMaxLength(tests, 'host')
        def text = ""
        text += "\nSequential Write Throughput per Node\n"
        text += "-------------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tMin        \tMax        \tMean       \tSum\n"
        for (def test in tests) {
            text += test['host'].padRight(maxHostLength, " ") + '\t'
            if (test['error']) {
                text += test['error'] + '\n'
            } else {
                text += test['minDiskSeqWriteInKBperSecond'] + ' KB/s\t' + test['maxDiskSeqWriteInKBperSecond'] + ' KB/s\t' + test['meanDiskSeqWriteInKBperSecond'] + ' KB/s\t' + test['sumDiskSeqWriteInKBperSecond'] + ' KB/s\n'
            }
        }

        text += "\nSequential Read Throughput per Node\n"
        text += "-------------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tMin        \tMax        \tMean       \tSum\n"
        for (def test in tests) {
            text += test['host'].padRight(maxHostLength, " ") + '\t'
            if (test['error']) {
                text += test['error'] + '\n'
            } else {
                text += test['minDiskSeqReadInKBperSecond'] + ' KB/s\t' + test['maxDiskSeqReadInKBperSecond'] + ' KB/s\t' + test['meanDiskSeqReadInKBperSecond'] + ' KB/s\t' + test['sumDiskSeqReadInKBperSecond'] + ' KB/s\n'
            }
        }

        text += "\nRandom Write Throughput per Node\n"
        text += "-------------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tMin        \tMax        \tMean       \tSum\n"
        for (def test in tests) {
            text += test['host'].padRight(maxHostLength, " ") + '\t'
            if (test['error']) {
                text += test['error'] + '\n'
            } else {
                text += test['minDiskRandomWriteInKBperSecond'] + ' KB/s\t' + test['maxDiskRandomWriteInKBperSecond'] + ' KB/s\t' + test['meanDiskRandomWriteInKBperSecond'] + ' KB/s\t' + test['sumDiskRandomWriteInKBperSecond'] + ' KB/s\n'
            }
        }


        text += "\nRandom Read Throughput per Node\n"
        text += "-------------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tMin        \tMax        \tMean       \tSum\n"
        for (def test in tests) {
            text += test['host'].padRight(maxHostLength, " ") + '\t'
            if (test['error']) {
                text += test['error'] + '\n'
            } else {
                text += test['minDiskRandomReadInKBperSecond'] + ' KB/s\t' + test['maxDiskRandomReadInKBperSecond'] + ' KB/s\t' + test['meanDiskRandomReadInKBperSecond'] + ' KB/s\t' + test['sumDiskRandomReadInKBperSecond'] + ' KB/s\n'
            }
        }

        def maxDiskLength = tests.collect { it.diskTests.collect { it.disk.size() } }.flatten().max() as int
        text += "\nPer disk throughput\n"
        text += "----------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tDisk".padRight(maxDiskLength, " ") + "\tSeqWrite    \tSeqRead     \tRandomWrite \tRandomRead\n"
        for (def test in tests) {
            if (test['error']) {
                text += test['host'].padRight(maxHostLength, " ") + '\t' + test['error'] + '\n'
            } else {
                for (def diskTest in test.diskTests) {
                    text += test['host'].padRight(maxHostLength, " ") + '\t' + diskTest['disk'].padRight(maxDiskLength, " ") + ' \t' + diskTest['seqWriteInKBperSecond'] + ' KB/s\t' + diskTest['seqReadInKBperSecond'] + ' KB/s\t' + diskTest['randomWriteInKBperSecond'] + ' KB/s\t' + diskTest['randomReadInKBperSecond'] + ' KB/s\n'
                }
            }
        }
        return text
    }


    def buildTextReportReadOnlyTests(tests) {
        def text = ""
        for (def test in tests) {
            def header = "Disk test - READONLY with ${test.dataInMB} MB\n"
            text += header
            text += "".padRight(header.size() + 40, "-") + "\n\n"
            text += "Min host throughput: ${test.minHostThroughputInMBperSecond} MB/s\n"
            text += "Max host throughput: ${test.maxHostThroughputInMBperSecond} MB/s\n"
            text += "Sum host throughput: ${test.sumHostThroughputInMBperSecond} MB/s\n"
            text += "Mean host throughput: ${test.meanHostThroughputInMBperSecond} MB/s\n"
            text += buildReadOnlyTestNodeStats(test.hostTests)
            //    text += buildReadOnlyTestDiskStats(test)
        }
        text
    }

    def buildReadOnlyTestNodeStats(tests) {
        def maxHostLength = getMaxLength(tests, 'host')
        def text = "\nCumulated disk throughput per node\n"
        text += "----------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tMin       \tMax       \tSum       \tMean\n"
        for (def test in tests) {
            if(test['error']) {
                text += test['host'].padRight(maxHostLength, " ") + '\t' + test['error'].substring(0, test['error'].indexOf('\n')) + '\n'
            }
            else {
                text += test['host'].padRight(maxHostLength, " ") + '\t' + test['minDiskThroughputInMBperSecond'] + ' MB/s\t' + test['maxDiskThroughputInMBperSecond'] + ' MB/s\t' + test['sumDiskThroughputInMBperSecond'] + ' MB/s\t' + test['meanDiskThroughputInMBperSecond'] + ' MB/s\n'
            }
        }

        def maxDiskLength = tests.collect { it.diskTests.collect { it.disk.size() } }.flatten().max() as int
        text += "\nPer disk throughput\n"
        text += "----------------------------------\n"
        text += "Host".padRight(maxHostLength, " ") + "\tDisk".padRight(maxDiskLength, " ") + "\tThroughput\n"
        for (def test in tests) {
            if (test['error']) {
                text += test['host'].padRight(maxHostLength, " ") + '\t' + test['error'].substring(0, test['error'].indexOf('\n')) + '\n'
            } else {
                for (def diskTest in test.diskTests) {
                    text += test['host'].padRight(maxHostLength, " ") + '\t' + diskTest['disk'].padRight(maxDiskLength, " ") + ' \t' + diskTest['throughputInMBperSecond'] + ' MB/s\n'
                }
            }
        }
        return text + '\n'
    }

    def getMaxLength(result, field) {
        def maxLength = 0
        for (def node in result) {
            if (maxLength < node[field].size()) {
                maxLength = node[field].size()
            }
        }
        return maxLength
    }

    def getRecommendations() {
        return ["Check that disks have expected speed.", "Check that disk speed is similar."]
    }

    def runDestroyTests(role) {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>

        def destroyTests = moduleConfig.tests.findAll { it.mode == "DESTROY" }
        if (destroyTests.size() > 0) {
            copyToolToRemoteHost(role, "iozone")
        }
        def nodes = globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role) }
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running DESTROY disks tests")
        log.info(">>>>> ... this can take some time.")
        destroyTests.each { readOnlyTest ->
            def dataInMB = readOnlyTest.getOrDefault("data_in_mb", 4096)
            def tests = []
            log.info(">>>>> ... test with ${dataInMB} MB")
            nodes.each { currentNode ->
                log.info(">>>>> ...... on node ${currentNode.host}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(currentNode.host)) {
                        def homePath = execute 'echo $HOME'

                        def node = [:]
                        def wardenStatus = executeSudo("service mapr-warden status > /dev/null 2>&1; echo \$?")
                        node['host'] = remote.host
                        if (wardenStatus.trim() != "4") {
                            node['error'] = "Node has a mapr-warden service available. For safety reasons, destructive tests will not run on nodes with MapR's Warden being installed."
                            tests.add(node)
                            return
                        }
                        def iozoneStatus = executeSudo("pgrep iozone; echo \$?")
                        if (iozoneStatus.trim() == "0") {
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
                        if (readResult.contains("Invalid argument")) {
                            node['error'] = "Cannot read disk: " + readResult
                            tests.add(node)
                            return
                        }
                        def diskTests = disks.collect { disk ->
                            def diskBasename = FilenameUtils.getBaseName(disk)
                            def content = executeSudo("cat ${homePath}/.clustercheck/${diskBasename}-iozone.log")
                            def tokens = content.tokenize('\n').find { it =~ /^[\d ]*$/ }.trim().tokenize().collect {
                                it as int
                            }
                            return [disk                    : disk,
                                    dataInKB                : tokens[0],
                                    reclen                  : tokens[1],
                                    seqWriteInKBperSecond   : tokens[2],
                                    seqReadInKBperSecond    : tokens[4],
                                    randomReadInKBperSecond : tokens[6],
                                    randomWriteInKBperSecond: tokens[7]
                            ]
                        }
                        node['minDiskSeqWriteInKBperSecond'] = diskTests.collect { it['seqWriteInKBperSecond'] }.min()
                        node['minDiskSeqReadInKBperSecond'] = diskTests.collect { it['seqReadInKBperSecond'] }.min()
                        node['minDiskRandomReadInKBperSecond'] = diskTests.collect {
                            it['randomReadInKBperSecond']
                        }.min()
                        node['minDiskRandomWriteInKBperSecond'] = diskTests.collect {
                            it['randomWriteInKBperSecond']
                        }.min()

                        node['maxDiskSeqWriteInKBperSecond'] = diskTests.collect { it['seqWriteInKBperSecond'] }.max()
                        node['maxDiskSeqReadInKBperSecond'] = diskTests.collect { it['seqReadInKBperSecond'] }.max()
                        node['maxDiskRandomReadInKBperSecond'] = diskTests.collect {
                            it['randomReadInKBperSecond']
                        }.max()
                        node['maxDiskRandomWriteInKBperSecond'] = diskTests.collect {
                            it['randomWriteInKBperSecond']
                        }.max()

                        node['sumDiskSeqWriteInKBperSecond'] = diskTests.sum { it['seqWriteInKBperSecond'] }
                        node['sumDiskSeqReadInKBperSecond'] = diskTests.sum { it['seqReadInKBperSecond'] }
                        node['sumDiskRandomReadInKBperSecond'] = diskTests.sum { it['randomReadInKBperSecond'] }
                        node['sumDiskRandomWriteInKBperSecond'] = diskTests.sum { it['randomWriteInKBperSecond'] }

                        node['meanDiskSeqWriteInKBperSecond'] = (node['sumDiskSeqWriteInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskSeqReadInKBperSecond'] = (node['sumDiskSeqReadInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskRandomReadInKBperSecond'] = (node['sumDiskRandomReadInKBperSecond'] / diskTests.size()) as int
                        node['meanDiskRandomWriteInKBperSecond'] = (node['sumDiskRandomWriteInKBperSecond'] / diskTests.size()) as int
                        node['diskTests'] = diskTests
                        tests.add(node)
                    }
                }
            }
            def testsWithOutError = tests.findAll{ !it['error'] }
            def testResult = [
                    dataInMB                        : dataInMB,
                    mode                            : "DESTROY",
                    sumNodeSeqWriteInKBperSecond    : testsWithOutError.sum { it['sumDiskSeqWriteInKBperSecond'] },
                    sumNodeSeqReadInKBperSecond     : testsWithOutError.sum { it['sumDiskSeqReadInKBperSecond'] },
                    sumNodeRandomReadInKBperSecond  : testsWithOutError.sum { it['sumDiskRandomReadInKBperSecond'] },
                    sumNodeRandomWriteInKBperSecond : testsWithOutError.sum { it['sumDiskRandomWriteInKBperSecond'] },
                    meanNodeSeqWriteInKBperSecond   : (testsWithOutError.sum {
                        it['sumDiskSeqWriteInKBperSecond']
                    } / testsWithOutError.size()) as int,
                    meanNodeSeqReadInKBperSecond    : (testsWithOutError.sum {
                        it['sumDiskSeqReadInKBperSecond']
                    } / testsWithOutError.size()) as int,
                    meanNodeRandomReadInKBperSecond : (testsWithOutError.sum {
                        it['sumDiskRandomReadInKBperSecond']
                    } / testsWithOutError.size()) as int,
                    meanNodeRandomWriteInKBperSecond: (testsWithOutError.sum {
                        it['sumDiskRandomWriteInKBperSecond']
                    } / testsWithOutError.size()) as int,
                    minNodeSeqWriteInKBperSecond    : testsWithOutError.collect { it['sumDiskSeqWriteInKBperSecond'] }.min(),
                    minNodeSeqReadInKBperSecond     : testsWithOutError.collect { it['sumDiskSeqReadInKBperSecond'] }.min(),
                    minNodeRandomReadInKBperSecond  : testsWithOutError.collect { it['sumDiskRandomReadInKBperSecond'] }.min(),
                    minNodeRandomWriteInKBperSecond : testsWithOutError.collect { it['sumDiskRandomWriteInKBperSecond'] }.min(),
                    maxNodeSeqWriteInKBperSecond    : testsWithOutError.collect { it['sumDiskSeqWriteInKBperSecond'] }.max(),
                    maxNodeSeqReadInKBperSecond     : testsWithOutError.collect { it['sumDiskSeqReadInKBperSecond'] }.max(),
                    maxNodeRandomReadInKBperSecond  : testsWithOutError.collect { it['sumDiskRandomReadInKBperSecond'] }.max(),
                    maxNodeRandomWriteInKBperSecond : testsWithOutError.collect { it['sumDiskRandomWriteInKBperSecond'] }.max(),
                    hostTests                       : tests]
            result.add(testResult)
        }
        log.info(">>>>> DESTROY disks tests finished")
        return result
    }

    def runReadOnlyTests(role) {
        def moduleConfig = globalYamlConfig.modules['benchmark-rawdisk'] as Map<String, ?>
        def readOnlyTests = moduleConfig.tests.findAll { it.mode == "READONLY" }
        def nodes = globalYamlConfig.nodes.findAll { role == "all" || it.roles.contains(role) }
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running READONLY disks tests")
        log.info(">>>>> ... this can take some time.")
        readOnlyTests.each { readOnlyTest ->
            def dataInMB = readOnlyTest.getOrDefault("data_in_mb", 4096)
            def tests = []
            log.info(">>>>> ... test with ${dataInMB} MB")
            nodes.forEach { currentNode ->
                log.info(">>>>> ...... on node ${currentNode.host}")
                ssh.run {
                    settings {
                        pty = true
                    }
                    session(ssh.remotes.role(currentNode.host)) {
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
                        if (readResult.contains("failed to open")) {
                            node['host'] = remote.host
                            node['error'] = readResult
                            tests.add(node)
                            return
                        }
                        def lines = readResult.tokenize('\n')
                        def diskTests = disks.collect { disk ->
                            def dataIdx = lines.findIndexOf { it.trim() == disk.trim() }
                            if (dataIdx == -1) {
                                return [:]
                            }
                            def res = [:]
                            res['disk'] = disk
                            def data = lines[dataIdx + 1].tokenize(',')
                            res['readBytes'] = data[0].trim().tokenize(' ')[0] as Long
                            res['timeInSeconds'] = data[1].trim().tokenize(' ')[0] as Double
                            res['throughputInMBperSecond'] = data[2].trim().tokenize(' ')[0] as Double
                            return res
                        }
                        node['host'] = remote.host
                        node['minDiskThroughputInMBperSecond'] = diskTests.collect {
                            it['throughputInMBperSecond']
                        }.min()
                        node['maxDiskThroughputInMBperSecond'] = diskTests.collect {
                            it['throughputInMBperSecond']
                        }.max()
                        node['sumDiskThroughputInMBperSecond'] = diskTests.sum { it['throughputInMBperSecond'] }
                        node['meanDiskThroughputInMBperSecond'] = (node['sumDiskThroughputInMBperSecond'] / diskTests.size())
                        node['diskTests'] = diskTests

                        tests.add(node)
                    }
                }
            }
            def testsWithOutError = tests.findAll{ !it['error'] }
            def testHost = [dataInMB                       : dataInMB,
                            mode                           : "READONLY",
                            minHostThroughputInMBperSecond : testsWithOutError.collect {
                                it['minDiskThroughputInMBperSecond']
                            }.min(),
                            maxHostThroughputInMBperSecond : testsWithOutError.collect {
                                it['maxDiskThroughputInMBperSecond']
                            }.max(),
                            sumHostThroughputInMBperSecond : testsWithOutError.sum { it['sumDiskThroughputInMBperSecond'] },
                            meanHostThroughputInMBperSecond: (testsWithOutError.sum {
                                it['meanDiskThroughputInMBperSecond']
                            } / testsWithOutError.size()),
                            hostTests                      : tests]
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
