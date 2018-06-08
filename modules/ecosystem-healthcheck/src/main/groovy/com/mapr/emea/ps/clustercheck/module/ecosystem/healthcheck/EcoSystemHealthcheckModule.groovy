package com.mapr.emea.ps.clustercheck.module.ecosystem.healthcheck

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "ecosystem-healthcheck", version = "1.0")
class EcoSystemHealthcheckModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(EcoSystemHealthcheckModule.class);

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
    List<String> validate() throws ModuleValidationException {
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def clusterconfigaudit = globalYamlConfig.modules['ecosystem-healthcheck'] as Map<String, ?>
        def role = clusterconfigaudit.getOrDefault("role", "all")
        log.info(">>>>> Running ecosystem-healthcheck")
        def result = Collections.synchronizedList([])
        def packages = retrievePackages(role)
        def hostsWithDrill = findHostsWithPackage(packages, "mapr-drill")
        println(hostsWithDrill)
        List recommendations = calculateRecommendations(result)
        def textReport = buildTextReport(result)
        log.info(">>>>> ... ecosystem-healthcheck finished")
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: recommendations)
    }

    def List<Object> findHostsWithPackage(List packages, packageDrill) {
        packages.findAll { it['mapr.packages'].find { it.contains(packageDrill) } != null }.collect { it['host'] }
    }

    def retrievePackages(role) {
        def packages = Collections.synchronizedList([])
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
                node['host'] = remote.host
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                if (distribution.toLowerCase().contains("ubuntu")) {
                    node['mapr.packages'] = executeSudo('apt list --installed | grep mapr').tokenize('\n')
                } else {
                    node['mapr.packages'] = executeSudo('rpm -qa | grep mapr').tokenize('\n')
                }
                packages.add(node)
            }
        }
        return packages
    }

    def List calculateRecommendations(def groupedResult) {
        def recommendations = []
        // TODO
        recommendations
    }

    def buildTextReport(result) {
        def text = ""
        // TODO
        text += "TODO"
        text
    }

}
