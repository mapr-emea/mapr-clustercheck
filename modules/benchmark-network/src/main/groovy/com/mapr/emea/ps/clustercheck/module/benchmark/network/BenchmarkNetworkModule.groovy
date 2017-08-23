package com.mapr.emea.ps.clustercheck.module.benchmark.network

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

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

        return new ClusterCheckResult(reportJson: [not: "implemented"], reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
//        return [firstName:'John', lastName:'Doe', age:25]
    }
}
