package com.mapr.emea.ps.clustercheck.module.clusterconfigaudit

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
// TODO implement TEXT report
// TODO implement diffs and give recommendations

// custom module?
// TODO grab yarn config
// TODO grab env.sh
// TODO grab hadoop config
// TODO grab mfs config
// TODO grab zooconf config
// TODO grab storage pools
// TODO grab disks
// TODO grab clusters.conf
// TODO grab check truststores with clusters conf
// TODO grab ecosystem components


@ClusterCheckModule(name = "clusterconfigaudit", version = "1.0")
class ClusterConfigAuditModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(ClusterConfigAuditModule.class);

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
        def clusterconfigaudit = globalYamlConfig.modules.clusterconfigaudit as Map<String, ?>
        def role = clusterconfigaudit.getOrDefault("role", "all")
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running cluster-config-audit")
        ssh.run {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]

                result.add(node)
            }
        }

//        echo Check for root ownership of /opt/mapr
//        clush $parg $parg2 'stat --printf="%U:%G %A %n\n" $(readlink -f /opt/mapr)'; echo $sep
        log.info(">>>>> ... cluster-config-audit finished")
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
    }


}
