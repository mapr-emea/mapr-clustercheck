package com.mapr.emea.ps.clustercheck.module.ecosystem.healthcheck

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemDrill

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "ecosystem-healthcheck", version = "1.0")
class EcoSystemHealthcheckModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(EcoSystemHealthcheckModule.class)

    @Autowired
    @Qualifier("ssh")
    def ssh

    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    @Autowired
    EcoSystemDrill drill


    // and more tests based on https://docs.google.com/document/d/1VpMDmvCDHcFz09P8a6rhEa3qFW5mFGFLVJ0K4tkBB0Q/edit
    def defaultTestMatrix = [
            [name: "drill-jdbc-file-json-plainauth", drill_port: 31010, enabled: false],
            [name: "drill-jdbc-file-json-maprsasl", drill_port: 31010, enabled: false],
            // TODO implement
            [name: "drill-jdbc-maprdb-json-plainauth", drill_port: 31010, enabled: false],
            [name: "drill-jdbc-maprdb-json-maprsasl", drill_port: 31010, enabled: false],
            [name: "drill-ui", drill_ui_port: 8047, enabled: false],
            [name: "maprdb-json-shell", enabled: false],
            [name: "maprdb-binary-shell" , enabled: false],
            [name: "maprlogin-auth", enabled: false],
            [name: "maprfs" , enabled: false],
            [name: "mapr-streams" , enabled: false],
            [name: "kafka-rest-plainauth" , enabled: false],
            [name: "kafka-rest-maprsasl" , enabled: false],
            [name: "httpfs-maprsasl" , enabled: false],
            [name: "httpfs-plainauth" , enabled: false],
            [name: "hue-plainauth", enabled: false],
            [name: "yarn-resourcemanager-ui-plainauth", enabled: false],
            [name: "yarn-resourcemanager-ui-maprsasl", enabled: false],
            [name: "yarn-resourcemanager-maprsasl", enabled: false],
            [name: "hive-server-plainauth", enabled: false],
            [name: "hive-server-maprsasl" , enabled: false],
            [name: "hive-metastore-maprsasl" , enabled: false],
            [name: "spark-yarn" , enabled: false],
            [name: "spark-historyserver-ui", enabled: false],
            [name: "sqoop1", enabled: false],
            [name: "sqoop2", enabled: false],
            [name: "elasticsearch", enabled: false],
            [name: "kibana-ui", enabled: false],
            [name: "opentsdb", enabled: false],
            [name: "grafana-ui", enabled: false],
            [name: "oozie-client", enabled: false],
            [name: "oozie-ui", enabled: false],
    ]

    @Override
    Map<String, ?> yamlModuleProperties() {
        return [username: "mapr", password: "mapr", ticketfile: "/opt/mapr/conf/mapruserticket", tests:defaultTestMatrix]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def healthcheckconfig = globalYamlConfig.modules['ecosystem-healthcheck'] as Map<String, ?>
        def role = healthcheckconfig.getOrDefault("role", "all")

        log.info(">>>>> Running ecosystem-healthcheck")

        def result = Collections.synchronizedMap([:])
        def packages = ecoSystemHealthcheckUtil.retrievePackages(role)

        healthcheckconfig['tests'].each { test ->
            log.info(">>>>>>> Running test '${test['name']}'")

            if(test['name'] == "drill-jdbc-file-json-plainauth" && (test['enabled'] as boolean)) {
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", "/opt/mapr/conf/mapruserticket")
                def username = healthcheckconfig.getOrDefault("username", "mapr")
                def password = healthcheckconfig.getOrDefault("password", "mapr")
                def port = healthcheckconfig.getOrDefault("drill_port", 31010)
                result['drill-jdbc-file-json-plainauth'] = drill.verifyDrillJdbcPlainAuth(packages, ticketfile, username, password, port)
            } else if(test['name'] == "drill-jdbc-file-json-maprsasl" && (test['enabled'] as boolean)) {
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", "/opt/mapr/conf/mapruserticket")
                def port = healthcheckconfig.getOrDefault("drill_port", 31010)
                result['drill-jdbc-file-json-maprsasl'] = drill.verifyDrillJdbcMaprSasl(packages, ticketfile, port)
            }
            else {
                log.error("       Test with name '${test['name']}' not found!")
            }
        }
        List recommendations = calculateRecommendations(result)
        def textReport = buildTextReport(result)

        log.info(">>>>> ... ecosystem-healthcheck finished")
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: recommendations)
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
