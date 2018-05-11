package com.mapr.emea.ps.clustercheck.module.benchmark.network.iperf

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
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

/**
 * This test checks the network performance.
 *
 * Parameters:
 *    threads: Defines the number of client thread for parallel execution
 *    data_per_thread: The data which is per thread. You can use 'M' for Megabytes, 'G' for Gigabytes and 'T' for Terabytes.
 *    mode: Defines the how many clients are executed against the servers
 *         mode=all Every node connects to every node
 *         mode=split Nodes are splitted into two halfs, first half is server and second half is client
 *         mode=role Server and client is defined by role 'benchmark_network_iperf_server' and 'benchmark_network_iperf_client'. Every client connects to every server.
 */
@ClusterCheckModule(name = "benchmark-network-iperf", version = "1.0")
class BenchmarkNetworkIperfModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkNetworkIperfModule.class);

    def defaultTestMatrix = [
            [threads: 1, data_per_thread: "4096M", mode: "split"],
            [threads: 2, data_per_thread: "2048M", mode: "split"],
            [threads: 4, data_per_thread: "1024M", mode: "split"],
            [threads: 8, data_per_thread: "512M", mode: "split"]
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
        if(globalYamlConfig.nodes.size() < 2) {
            throw new ModuleValidationException("Please ensure that you have at least two nodes for network test available. Otherwise disable the module.")
        }
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def moduleConfig = globalYamlConfig.modules['benchmark-network-iperf'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        checkIfIperfIsRunningAndStop(role)
        copyIperfToRemoteHost(role)
        startIperfServer(role)
        def result = runIperfTests(role)
        def textReport = buildTextReport(result)
        stopIperfServer(role)
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: ["Check for larger differences."])
    }

    def buildTextReport(def result) {
        def text = ""
        for(def res in result) {
            text += "Data per thread: ${res.dataPerThread} - Threads: ${res.threads}\n"
            for(def test in res.tests) {
                text += "> From ${test.clientHost} to ${test.serverHost}: ${(test.throughputInBitsPerSecond / 1024 / 1024).setScale(3, RoundingMode.HALF_UP)} MBit/s (${(test.dataCopiedInBytes / 1024 / 1024).setScale(3, RoundingMode.HALF_UP)} MB data copied)\n"
            }
            text += "-----------------------------------------------------------------------\n"
        }
        text
    }

    def runIperfTests(role) {
        def moduleConfig = globalYamlConfig.modules['benchmark-network-iperf'] as Map<String, ?>
        def testMatrix = moduleConfig.getOrDefault("tests", defaultTestMatrix)

        def result = Collections.synchronizedList([])
        // only one command executed with runInOrder
        testMatrix.each { matrixItem ->
            def iperfTests = Collections.synchronizedList([])
            log.info(">>>>> Running iperf tests - Threads: ${matrixItem.threads} - Data per thread: ${matrixItem.data_per_thread}")
            log.info(">>>>> ... this can take some time.")
            if (matrixItem.mode == "all") {
                // run all nodes against all others
                globalYamlConfig.nodes.each { server ->
                    globalYamlConfig.nodes.each { client ->
                        if(server != client) {
                            log.info(">>>>> ..... executing client ${client.host} to server ${server.host}.")
                            ssh.run {
                                session(ssh.remotes.role(client.host)) {
                                    // client calls
                                    def exec = { arg -> execute(arg) }
                                    iperfTests << runSingleTest(remote, server, matrixItem, exec)
                                }
                            }
                        }
                    }
                }
            } else if (matrixItem.mode == "split") {
                // run half nodes against the other half
                def numberOfNodes = globalYamlConfig.nodes.size()
                def firstHalfEndIndex = ((numberOfNodes / 2) as int)
                def hosts = globalYamlConfig.nodes //.collect{ it.host }
                def firstHalf = hosts.subList(0, firstHalfEndIndex)
                // second half can have one more element
                def secondHalf = hosts.subList(firstHalfEndIndex, hosts.size())
                def currentIdx = 0
                secondHalf.each { client ->
                    def server = firstHalf[currentIdx < firstHalf.size() ? currentIdx++ : (firstHalf.size() - 1)]
                    log.info(">>>>> ..... executing client ${client.host} to server ${server.host}.")
                    ssh.run {
                        session(ssh.remotes.role(client.host)) {
                            // client calls
                            def exec = { arg -> execute(arg) }
                            iperfTests <<  runSingleTest(remote, server, matrixItem, exec)
                        }
                    }
                }
            }  else if (matrixItem.mode == "role") {
                // run all nodes against all others
                globalYamlConfig.nodes.findAll{ it.roles.contains("benchmark_network_iperf_server") }.each { server ->
                    globalYamlConfig.nodes.findAll{ it.roles.contains("benchmark_network_iperf_client") }.each { client ->
                        if(server != client) {
                            log.info(">>>>> ..... executing client ${client.host} to server ${server.host}.")
                            ssh.run {
                                session(ssh.remotes.role(client.host)) {
                                    // client calls
                                    def exec = { arg -> execute(arg) }
                                    iperfTests << runSingleTest(remote, server, matrixItem, exec)
                                }
                            }
                        }
                    }
                }
            } else {
                log.error(">>>>> ... nothing executed, because mode '${matrixItem.mode}' is unknown.")
            }
            result << [dataPerThread: matrixItem.data_per_thread, threads: matrixItem.threads, tests: iperfTests]
        }
        return result
    }

    def runSingleTest(remote, node, matrixItem, execute) {
        def currentHost = remote.host
        def serverHost = node.host
        def iperfResult = execute "\$HOME/.clustercheck/iperf -c ${serverHost} -n ${matrixItem.data_per_thread} -P ${matrixItem.threads} -y C"
        def iperfTokens = iperfResult.tokenize(',')
        def dataCopiedInBytes = Long.valueOf(iperfTokens[-2])
        def throughputInBitPerSecond = Long.valueOf(iperfTokens[-1])
        sleep(1000)
        return [serverHost: serverHost, clientHost: currentHost, dataCopiedInBytes: dataCopiedInBytes as Long, throughputInBitsPerSecond: throughputInBitPerSecond as Long]
    }

    def checkIfIperfIsRunningAndStop(role) {
        log.info(">>>>> Check for running iperf instances")
        ssh.run {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def pid = executeSudo "pidof iperf | xargs echo" // xargs echo get rid of return code
                if (pid) {
                    execute "kill -9 ${pid}"
                }
            }
        }
        sleep(2000)
    }

    def stopIperfServer(role) {
        log.info(">>>>> Stopping iperf server on all nodes")
        ssh.run {
            settings {
                pty = true
            }
            session(ssh.remotes.role(role)) {
                executeSudo "pidof iperf | xargs kill -9"
            }
        }
        log.info(">>>>> Wait for 5 seconds")
        sleep(5000)
    }

    def startIperfServer(role) {
        log.info(">>>>> Starting iperf server on all nodes")
        ssh.run {
            session(ssh.remotes.role(role)) {
                execute 'nohup $HOME/.clustercheck/iperf -s > /dev/null 2>&1 &'
            }

        }
        log.info(">>>>> Wait for 5 seconds")
        sleep(5000)
    }

    def copyIperfToRemoteHost(role) {
        log.info(">>>>> Copy iperf to remote hosts")
        ssh.run {
            session(ssh.remotes.role(role)) {
                def homePath = execute 'echo $HOME'
                execute "mkdir -p ${homePath}/.clustercheck"
                def iperfInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/network/iperf/iperf").getInputStream()
                put from: iperfInputStream, into: "${homePath}/.clustercheck/iperf"
                execute "chmod +x ${homePath}/.clustercheck/iperf"
            }
        }
    }
}
