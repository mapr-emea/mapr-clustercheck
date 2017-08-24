package com.mapr.emea.ps.clustercheck.module.benchmark.network

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "benchmark-network", version = "1.0")
class BenchmarkNetworkModule implements ExecuteModule {
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
        return [:]
    }

    @Override
    void validate() throws ModuleValidationException {
        // TODO implement check with hostname
    }

    @Override
    ClusterCheckResult execute() {
        def clusteraudit = globalYamlConfig.modules['benchmark-network'] as Map<String, ?>
        def role = clusteraudit.getOrDefault("role", "all")
        copyIperfToRemoteHost(role)


        return new ClusterCheckResult(reportJson: [not: "implemented"], reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
//        return [firstName:'John', lastName:'Doe', age:25]
    }

    def copyIperfToRemoteHost(role) {
        ssh.run {
            session(ssh.remotes.role(role)) {
                execute 'mkdir -p $HOME/.clustercheck'
                def path = execute 'echo $HOME'
                def iperfInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/benchmark/network/iperf").getInputStream()
                put from: iperfInputStream, into: "${path}/.clustercheck/iperf"
                execute "chmod +x ${path}/.clustercheck/iperf"
            }
        }
    }
}
