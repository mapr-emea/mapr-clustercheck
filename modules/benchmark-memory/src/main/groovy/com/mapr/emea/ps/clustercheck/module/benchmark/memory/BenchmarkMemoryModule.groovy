package com.mapr.emea.ps.clustercheck.module.benchmark.memory

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
@ClusterCheckModule(name = "benchmark-memory", version = "1.0")
class BenchmarkMemoryModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(BenchmarkMemoryModule.class);

    def defaultTestMatrix = [
            [threads: 1, data_per_thread: "4096M"],
            [threads: 2, data_per_thread: "2048M"],
            [threads: 4, data_per_thread: "1024M"],
            [threads: 8, data_per_thread: "512M"]
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
        def moduleConfig = globalYamlConfig.modules['benchmark-memory'] as Map<String, ?>
        def role = moduleConfig.getOrDefault("role", "all")
        copyToolToRemoteHost(role, "lat_mem_rd")
        copyToolToRemoteHost(role, "stream59")
        def result = runMemoryTests(role)
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
    }

    def runMemoryTests(role) {
        //def moduleConfig = globalYamlConfig.modules['benchmark-memory'] as Map<String, ?>
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running memory tests")
        log.info(">>>>> ... this can take some time.")
        ssh.run {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                sleep(1000)
                def node = [:]
                node['hostname'] = execute('hostname -f')
                node['sockets'] = execute("grep '^physical' /proc/cpuinfo | sort -u | grep -c ^") as Integer
                node['cores'] = execute("grep '^cpu cores' /proc/cpuinfo | sort -u | awk '{print \$NF}'") as Integer
                node['threads'] = execute("grep '^siblings' /proc/cpuinfo | sort -u | awk '{print \$NF}'") as Integer
                def stream59Result = ""
                if (node['cores'] == node['threads']) {
                    stream59Result = execute("\$HOME/.clustercheck/stream59")
                } else {
                    def threads = node['sockets'] * node['cores']
                    stream59Result = execute("OMP_NUM_THREADS=${threads} KMP_AFFINITY=granularity=core,scatter \$HOME/.clustercheck/stream59")
                }
                node['memoryTest'] = [:]
                def copy = getColonValueFromLines(stream59Result, "Copy:")
                if (copy) {
                    def copyTokens = copy.tokenize(" ")
                    node['memoryTest']['copy'] = [:]
                    node['memoryTest']['copy']['rateInMBperSecond'] = copyTokens[0].trim()
                    node['memoryTest']['copy']['avgTime'] = copyTokens[1].trim()
                    node['memoryTest']['copy']['minTime'] = copyTokens[2].trim()
                    node['memoryTest']['copy']['maxTime'] = copyTokens[3].trim()
                }
                def scale = getColonValueFromLines(stream59Result, "Scale:")
                if (scale) {
                    def scaleTokens = scale.tokenize(" ")
                    node['memoryTest']['scale'] = [:]
                    node['memoryTest']['scale']['rateInMBperSecond'] = scaleTokens[0].trim()
                    node['memoryTest']['scale']['avgTime'] = scaleTokens[1].trim()
                    node['memoryTest']['scale']['minTime'] = scaleTokens[2].trim()
                    node['memoryTest']['scale']['maxTime'] = scaleTokens[3].trim()
                }
                def add = getColonValueFromLines(stream59Result, "Add:")
                if (add) {
                    def addTokens = add.tokenize(" ")
                    node['memoryTest']['add'] = [:]
                    node['memoryTest']['add']['rateInMBperSecond'] = addTokens[0].trim()
                    node['memoryTest']['add']['avgTime'] = addTokens[1].trim()
                    node['memoryTest']['add']['minTime'] = addTokens[2].trim()
                    node['memoryTest']['add']['maxTime'] = addTokens[3].trim()
                }
                def triad = getColonValueFromLines(stream59Result, "Triad:")
                if (triad) {
                    def triadTokens = triad.tokenize(" ")
                    node['memoryTest']['triad'] = [:]
                    node['memoryTest']['triad']['rateInMBperSecond'] = triadTokens[0].trim()
                    node['memoryTest']['triad']['avgTime'] = triadTokens[1].trim()
                    node['memoryTest']['triad']['minTime'] = triadTokens[2].trim()
                    node['memoryTest']['triad']['maxTime'] = triadTokens[3].trim()
                }
                result.add(node)
            }
        }
        log.info(">>>>> Memory tests finished")
        return result
    }

    def copyToolToRemoteHost(role, tool) {
        log.info(">>>>> Copy ${tool} to remote hosts")
        ssh.run {
            session(ssh.remotes.role(role)) {
                def homePath = execute 'echo $HOME'
                execute "mkdir -p ${homePath}/.clustercheck"
                def toolInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/memory/" + tool).getInputStream()
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
