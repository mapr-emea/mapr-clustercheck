package com.mapr.emea.ps.clustercheck.module.ecosystem.healthcheck

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRDB
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRLogin
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRStreams
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMfs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemDataAccessGateway
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemDrill
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMcs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemHttpfs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemKafkaRest
import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "ecosystem-healthcheck", version = "1.0")
class MapRComponentHealthcheckModule implements ExecuteModule {
    
    static final Logger log = LoggerFactory.getLogger(MapRComponentHealthcheckModule.class)
    
    static final String PATH_TICKET_FILE = "/opt/mapr/conf/mapruserticket"
    static final String PATH_SSL_CERTIFICATE_FILE = "/opt/mapr/conf/ssl_truststore.pem"

    static final String DEFAULT_MAPR_USERNAME = "mapr"
    static final String DEFAULT_MAPR_PASSWORD = "mapr123"

    static final Integer DEFAULT_DRILL_PORT = 31010
    static final Integer DEFAULT_DRILL_UI_PORT = 8047
    static final Integer DEFAULT_MCS_UI_PORT = 8443
    static final Integer DEFAULT_KAFKA_REST_PORT = 8082
    static final Integer DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT = 8243
    static final Integer DEFAULT_HTTPFS_PORT = 14000

    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    @Autowired
    CoreMfs coreMfs

    @Autowired
    CoreMapRDB coreMapRDB

    @Autowired
    CoreMapRStreams coreMapRStreams

    @Autowired
    CoreMcs coreMcs

    @Autowired
    CoreMapRLogin coreMapRLogin

    @Autowired
    EcoSystemDrill ecoSystemDrill

    @Autowired
    EcoSystemKafkaRest ecoSystemKafkaRest

    @Autowired
    EcoSystemDataAccessGateway ecoSystemDataAccessGateway

    @Autowired
    EcoSystemHttpfs ecoSystemHttpfs



    // and more tests based on https://docs.google.com/document/d/1VpMDmvCDHcFz09P8a6rhEa3qFW5mFGFLVJ0K4tkBB0Q/edit
    def defaultTestMatrix = [
            [name: "maprfs", ticketfile: "/opt/mapr/conf/mapruserticket", enabled: false],
            [name: "maprdb-json-shell", enabled: false],
            [name: "maprdb-binary-shell", enabled: false],
            [name: "mapr-streams", ticketfile: "/opt/mapr/conf/mapruserticket", enabled: false],
            [name: "mcs-ui-secure-pam", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, mcs_ui_port: DEFAULT_MCS_UI_PORT, enabled: false],
            [name: "mcs-ui-secure-ssl", certificate: PATH_SSL_CERTIFICATE_FILE, mcs_ui_port: DEFAULT_MCS_UI_PORT, enabled: false],
            [name: "mcs-ui-insecure", mcs_ui_port: DEFAULT_MCS_UI_PORT, enabled: false],  //TODO test
            [name: "maprlogin-password", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, enabled: false],
            [name: "drill-jdbc-jsonfile-plainauth", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-jdbc-file-json-maprsasl", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-jdbc-maprdb-json-plainauth", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-jdbc-maprdb-json-maprsasl", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-ui-secure-ssl", certificate: PATH_SSL_CERTIFICATE_FILE, drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false],
            [name: "drill-ui-secure-pam", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false],
            [name: "drill-ui-insecure", drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false], //TODO test
            [name: "kafka-rest-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false],
            [name: "kafka-rest-auth-insecure", kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false], //TODO test
            [name: "data-access-gateway-rest-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, data_access_gateway_rest_port: DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT, enabled: false],
            [name: "data-access-gateway-rest-auth-insecure", data_access_gateway_rest_port: DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT, enabled: false], //TODO test
            [name: "httpfs-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, httpfs_port: DEFAULT_HTTPFS_PORT, enabled: false],
            [name: "httpfs-auth-insecure" , enabled: false],  //TODO test

            // TODO implement
            [name: "mapr-cmd-maprcli-api", enabled: false], // https://mapr.com/docs/home/ReferenceGuide/maprcli-REST-API-Syntax.html
            [name: "mapr-cmd-rest-api", enabled: false],
            [name: "kafka-rest-api-ssl-pam", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false],
            [name: "data-access-gateway-rest-api-pam-ssl" , enabled: false],
            [name: "data-access-gateway-grpc" , enabled: false], //TODO python & node.js
            [name: "kafka-connect-maprsasl" , enabled: false],
            [name: "httpfs-auth-certificate-ssl", certificate: PATH_SSL_CERTIFICATE_FILE, httpfs_port: DEFAULT_HTTPFS_PORT, enabled: false], //TODO https://mapr.com/docs/home/HttpFS/SSLSecurityforHttpFS.html
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
        return [username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, ticketfile: PATH_TICKET_FILE, tests:defaultTestMatrix]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        return []
    }

    @Override
    ClusterCheckResult execute() {

        log.trace("Start : MapRComponentHealthcheckModule : ClusterCheckResult")

        def healthcheckconfig = globalYamlConfig.modules['ecosystem-healthcheck'] as Map<String, ?>
        def role = healthcheckconfig.getOrDefault("role", "all")

        log.info(">>>>> Running ecosystem-healthcheck")

        def result = Collections.synchronizedMap([:])
        def packages = mapRComponentHealthcheckUtil.retrievePackages(role)

        healthcheckconfig['tests'].each { test ->

            log.info(">>>>>>> Running test '${test['name']}'")

            if(test['name'] == "maprfs" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                result['maprfs'] = coreMfs.verifyMaprFs(packages, ticketfile)

            } else if(test['name'] == "maprdb-json-shell" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                result['maprdb-json-shell'] = coreMapRDB.verifyMapRDBJsonShell(packages, ticketfile)

            } else if(test['name'] == "maprdb-binary-shell" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                result['maprdb-binary-shell'] = coreMapRDB.verifyMapRDBBinaryShell(packages, ticketfile)

            } else if(test['name'] == "mapr-streams" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                result['mapr-streams'] = coreMapRStreams.verifyMapRStreams(packages, ticketfile)

            } else if(test['name'] == "mcs-ui-secure-pam" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("mcs_ui_port", DEFAULT_MCS_UI_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                result['mcs-ui-secure-pam'] = coreMcs.verifyMcsUiSecurePAM(packages, username, password, port)

            } else if(test['name'] == "mcs-ui-secure-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("mcs_ui_port", DEFAULT_MCS_UI_PORT)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)
                result['mcs-ui-secure-ssl'] = coreMcs.verifyMcsUiSecureSSL(packages, certificate, port)

            } else if(test['name'] == "mcs-ui-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("mcs_ui_port", DEFAULT_MCS_UI_PORT)
                result['mcs-ui-insecure'] = coreMcs.verifyMcsUiInSecure(packages, port)

            } else if(test['name'] == "maprlogin-password" && (test['enabled'] as boolean)) {

                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                result['maprlogin-password'] = coreMapRLogin.verifyMapRLoginPassword(packages, username, password)

            } else if(test['name'] == "drill-jdbc-jsonfile-plainauth" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def port = healthcheckconfig.getOrDefault("drill_port", DEFAULT_DRILL_PORT)
                result['drill-jdbc-jsonfile-plainauth'] = ecoSystemDrill.verifyDrillJdbcJsonFilePlainAuth(packages, ticketfile, username, password, port)

            } else if(test['name'] == "drill-jdbc-file-json-maprsasl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", DEFAULT_DRILL_PORT)
                def port = healthcheckconfig.getOrDefault("drill_port", DEFAULT_DRILL_PORT)
                result['drill-jdbc-file-json-maprsasl'] = ecoSystemDrill.verifyDrillJdbcMaprSasl(packages, ticketfile, port)

            } else if(test['name'] == "drill-jdbc-maprdb-json-plainauth" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def port = healthcheckconfig.getOrDefault("drill_port", DEFAULT_DRILL_PORT)
                result['drill-jdbc-maprdb-json-plainauth'] = ecoSystemDrill.verifyDrillJdbcMaprdbJsonPlainAuth(packages, ticketfile, username, password, port)

            } else if(test['name'] == "drill-jdbc-maprdb-json-maprsasl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                def port = healthcheckconfig.getOrDefault("drill_port", DEFAULT_DRILL_PORT)
                result['drill-jdbc-maprdb-json-maprsasl'] = ecoSystemDrill.verifyDrillJdbcMaprdbJsonMaprSasl(packages, ticketfile, port)

            } else if(test['name'] == "drill-ui-secure-ssl" && (test['enabled'] as boolean)) {

                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)
                def port = healthcheckconfig.getOrDefault("drill_ui_port", DEFAULT_DRILL_UI_PORT)
                result['drill-ui-secure-ssl'] = ecoSystemDrill.verifyDrillUISecureSSL(packages, certificate, port)

            } else if(test['name'] == "drill-ui-secure-pam" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("drill_ui_port", DEFAULT_DRILL_UI_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                result['drill-ui-secure-pam'] = ecoSystemDrill.verifyDrillUISecurePAM(packages, username, password, port)

            } else if(test['name'] == "drill-ui-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("drill_ui_port", DEFAULT_DRILL_UI_PORT)
                result['drill-ui-insecure'] = ecoSystemDrill.verifyDrillUIInsecure(packages, port)

            } else if(test['name'] == "kafka-rest-auth-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("kafka_rest_port", DEFAULT_KAFKA_REST_PORT)

                result['kafka-rest-auth-insecure'] = ecoSystemKafkaRest.verifyAuthInsecure(packages, port)

            } else if(test['name'] == "kafka-rest-auth-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("kafka_rest_port", DEFAULT_KAFKA_REST_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['kafka-rest-auth-pam-ssl'] = ecoSystemKafkaRest.verifyAuthPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "data-access-gateway-rest-auth-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("data_access_gateway_rest_port", DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT)

                result['data-access-gateway-rest-auth-insecure'] = ecoSystemDataAccessGateway.verifyRESTAuthInsecure(packages, port)

            } else if(test['name'] == "data-access-gateway-rest-auth-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("data_access_gateway_rest_port", DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['data-access-gateway-rest-auth-pam-ssl'] = ecoSystemDataAccessGateway.verifyRESTAuthPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "httpfs-auth-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("data_access_gateway_rest_port", DEFAULT_HTTPFS_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['httpfs-auth-pam-ssl'] = ecoSystemHttpfs.verifyAuthPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "httpfs-auth-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("data_access_gateway_rest_port", DEFAULT_HTTPFS_PORT)

                result['httpfs-auth-insecure'] = ecoSystemHttpfs.verifyAuthInsecure(packages, port)

            } else {
                log.info(">>>>> ... Test '${test['name']}' not found!")
            }
        }

        List recommendations = calculateRecommendations(result)
        def textReport = buildTextReport(result)

        log.info(">>>>> ... ecosystem-healthcheck finished")
        log.trace("End : MapRComponentHealthcheckModule : ClusterCheckResult")

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
