package com.mapr.emea.ps.clustercheck.module.clusterconfigaudit

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import groovy.json.JsonOutput
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
// TODO implement TEXT report
// TODO implement diffs and give recommendations

// TODO grab env.sh
// TODO grab spark-defaults
// TODO grab spark-env
// TODO grab hadoop config
// TODO grab mfs config
// TODO grab cldb config
// TODO grab warden config
// TODO grab zooconf config
// TODO grab storage pools
// TODO grab disks
// TODO grab clusters.conf
// TODO grab check truststores with clusters conf


@ClusterCheckModule(name = "cluster-config-audit", version = "1.0")
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
        def clusterconfigaudit = globalYamlConfig.modules['cluster-config-audit'] as Map<String, ?>
        def role = clusterconfigaudit.getOrDefault("role", "all")
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running cluster-config-audit")
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
                def exec = { arg -> execute(arg) }
                node['hostname'] = execute 'hostname -f'
                node['yarnsite'] = collectYarnSiteProperties(exec)
                node['coresite'] = collectCoreSiteProperties(exec)
                node['hivesite'] = collectHiveSiteProperties(exec)
                node['hiveenv'] = collectHiveEnv(exec)
                node['maprHomeOwnership'] = execute('stat --printf="%U:%G %A %n\\n" $(readlink -f /opt/mapr)')
                node['maprPackages'] = execute('rpm -qa | grep mapr').tokenize('\n')
                result.add(node)
            }
        }

        log.info(">>>>> ... cluster-config-audit finished")
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
    }


    def collectYarnSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/yarn-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectCoreSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/core-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHiveSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hive/hiveversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hive/hive-${hadoopVersion}/conf/hive-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHiveEnv(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hive/hiveversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            return execute("cat /opt/mapr/hive/hive-${hadoopVersion}/conf/hive-env.sh").trim()
        }
        return ""
    }


}
