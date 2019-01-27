package com.mapr.emea.ps.clustercheck.module.ecosystem.healthcheck

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRDB
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRStreams
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMapRTool
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMfs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemDataAccessGateway
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemDrill
import com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent.CoreMcs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemHive
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemHttpfs
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemKafkaRest
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemSpyglass
import com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent.EcoSystemYarn
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
    static final String DEFAULT_MAPR_SSL_TRUSTSTORE_FILE = "/opt/mapr/conf/ssl_truststore"
    static final String PATH_SSL_CERTIFICATE_FILE = "/opt/mapr/conf/ssl_truststore.pem"
    static final String PATH_SSL_CERTIFICATE_FILE_ELASTIC = "/opt/mapr/elasticsearch/elasticsearch-6.2.3/etc/elasticsearch/sg/admin-usr-clientCert.pem"
    static final String PATH_SSL_CERTIFICATE_FILE_KIBANA = "/opt/mapr/kibana/kibana-6.2.3/config/cert.pem"

    static final String DEFAULT_MAPR_USERNAME = "mapr"
    static final String DEFAULT_MAPR_PASSWORD = "mapr123"

    static final String DEFAULT_ELASTIC_USERNAME = "admin"
    static final String DEFAULT_ELASTIC_PASSWORD = "admin"

    static final String DEFAULT_KIBANA_USERNAME = "admin"
    static final String DEFAULT_KIBANA_PASSWORD = "admin"

    static final Integer DEFAULT_DRILL_PORT = 31010
    static final Integer DEFAULT_DRILL_UI_PORT = 8047
    static final Integer DEFAULT_RESOURCEMANAGER_SECURE_PORT = 8090
    static final Integer DEFAULT_RESOURCEMANAGER_INSECURE_PORT = 8088
    static final Integer DEFAULT_NODEMANAGER_SECURE_PORT = 8044
    static final Integer DEFAULT_NODEMANAGER_INSECURE_PORT = 8042
    static final Integer DEFAULT_YARN_HISTORY_SERVER_SECURE_PORT = 19890
    static final Integer DEFAULT_YARN_HISTORY_SERVER_INSECURE_PORT = 19888
    static final Integer DEFAULT_HIVE_SERVER_PORT = 10000
    static final Integer DEFAULT_HIVE_SERVER_UI_PORT = 10002
    static final Integer DEFAULT_HIVE_WEBHCAT_API_PORT = 50111
    static final Integer DEFAULT_MCS_UI_PORT = 8443
    static final Integer DEFAULT_KAFKA_REST_PORT = 8082
    static final Integer DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT = 8243
    static final Integer DEFAULT_HTTPFS_PORT = 14000
    static final Integer DEFAULT_OPENTSDB_API_PORT = 4242
    static final Integer DEFAULT_GRAFANA_UI_PORT = 3000
    static final Integer DEFAULT_ELASTIC_PORT = 9200
    static final Integer DEFAULT_KIBANA_PORT = 5601

    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    //TODO use tmpPath
    @Autowired
    @Qualifier("localTmpDir")
    String tmpPath

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
    CoreMapRTool coreMapRTool

    @Autowired
    EcoSystemDrill ecoSystemDrill

    @Autowired
    EcoSystemYarn ecoSystemYarn

    @Autowired
    EcoSystemHive ecoSystemHive

    @Autowired
    EcoSystemKafkaRest ecoSystemKafkaRest

    @Autowired
    EcoSystemDataAccessGateway ecoSystemDataAccessGateway

    @Autowired
    EcoSystemHttpfs ecoSystemHttpfs

    @Autowired
    EcoSystemSpyglass ecoSystemSpyglass


    //TODO show all query strings in results

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
            [name: "mapr-maprcli-api-sasl", ticketfile: "/opt/mapr/conf/mapruserticket", enabled: false],
            [name: "drill-jdbc-jsonfile-plainauth", drill_port: DEFAULT_DRILL_PORT, enabled: false], //TODO need to optimized the packages check
            [name: "drill-jdbc-file-json-maprsasl", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-jdbc-maprdb-json-plainauth", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-jdbc-maprdb-json-maprsasl", drill_port: DEFAULT_DRILL_PORT, enabled: false],
            [name: "drill-ui-secure-ssl", certificate: PATH_SSL_CERTIFICATE_FILE, drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false],
            [name: "drill-ui-secure-pam", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false],
            [name: "drill-ui-insecure", drill_ui_port: DEFAULT_DRILL_UI_PORT, enabled: false], //TODO test
            [name: "yarn-resourcemanager-ui-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, resource_manager_secure_port: DEFAULT_RESOURCEMANAGER_SECURE_PORT, enabled: false],
            [name: "yarn-nodemanager-ui-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, node_manager_secure_port: DEFAULT_NODEMANAGER_SECURE_PORT, enabled: false],
            [name: "yarn-resourcemanager-ui-insecure", resource_manager_insecure_port: DEFAULT_RESOURCEMANAGER_INSECURE_PORT, enabled: false], //TODO test
            [name: "yarn-nodemanager-ui-insecure", node_manager_insecure_port: DEFAULT_NODEMANAGER_INSECURE_PORT, enabled: false], //TODO test
            [name: "yarn-command-maprsasl", ticketfile: "/opt/mapr/conf/mapruserticket", enabled: false],
            [name: "yarn-historyserver-ui-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, yarn_history_server_secure_port: DEFAULT_YARN_HISTORY_SERVER_SECURE_PORT, enabled: false],
            [name: "yarn-historyserver-ui-insecure", yarn_history_server_insecure_port: DEFAULT_YARN_HISTORY_SERVER_INSECURE_PORT, enabled: false], //TODO test
            [name: "hive-server-ui-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, hive_server_ui_port: DEFAULT_HIVE_SERVER_UI_PORT, enabled: false],
            [name: "hive-client-maprsasl", ticketfile: "/opt/mapr/conf/mapruserticket", enabled: false],
            [name: "hive-beeline-maprsasl", ticketfile: "/opt/mapr/conf/mapruserticket", hive_server_port: DEFAULT_HIVE_SERVER_PORT, enabled: false],//TODO hive to test with different version
            [name: "hive-beeline-pam", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, hive_server_port: DEFAULT_HIVE_SERVER_PORT, enabled: false], //TODO hive to test with different version
            [name: "hive-beeline-maprsasl-pam", ticketfile: "/opt/mapr/conf/mapruserticket", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, hive_server_port: DEFAULT_HIVE_SERVER_PORT, enabled: false], //TODO hive to test with different version
            [name: "hive-beeline-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, ssl_truststore_file: DEFAULT_MAPR_SSL_TRUSTSTORE_FILE, hive_server_port: DEFAULT_HIVE_SERVER_PORT, enabled: false],
            [name: "hive-webhcat-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, "hive_webhcat_api_port": DEFAULT_HIVE_WEBHCAT_API_PORT, enabled: false],
            [name: "kafka-rest-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false],
            [name: "kafka-rest-auth-insecure", kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false], //TODO test
            [name: "kafka-rest-api-pam-ssl", ticketfile: "/opt/mapr/conf/mapruserticket", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, kafka_rest_port: DEFAULT_KAFKA_REST_PORT, enabled: false],
            [name: "data-access-gateway-rest-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, data_access_gateway_rest_port: DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT, enabled: false],
            [name: "data-access-gateway-rest-auth-insecure", data_access_gateway_rest_port: DEFAULT_DATA_ACCESS_GATEWAY_REST_PORT, enabled: false], //TODO test
            [name: "httpfs-auth-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, httpfs_port: DEFAULT_HTTPFS_PORT, enabled: false],
            [name: "httpfs-auth-insecure,", httpfs_port: DEFAULT_HTTPFS_PORT, enabled: false],  //TODO test
            [name: "opentsdb-api", opentsdb_api_port: DEFAULT_OPENTSDB_API_PORT, enabled: false],
            [name: "grafana-ui-pam-ssl", username: DEFAULT_MAPR_USERNAME, password: DEFAULT_MAPR_PASSWORD, certificate: PATH_SSL_CERTIFICATE_FILE, grafana_ui_port: DEFAULT_GRAFANA_UI_PORT, enabled: false],
            [name: "elasticsearch-healthcheck-pam-ssl", username_elastic: DEFAULT_ELASTIC_USERNAME, password_elastic: DEFAULT_ELASTIC_PASSWORD, certificate_elastic: PATH_SSL_CERTIFICATE_FILE_ELASTIC, elastic_port: DEFAULT_ELASTIC_PORT, enabled: false],
            [name: "kibana-ui-pam-ssl", username_kibana: DEFAULT_KIBANA_USERNAME, password_kibana: DEFAULT_KIBANA_PASSWORD, certificate_kibana: PATH_SSL_CERTIFICATE_FILE_KIBANA, kibana_port: DEFAULT_KIBANA_PORT, enabled: false],


            // TODO implement
            // TODO How to simplify the template?
            [name: "drill-on-yarn-plainauth", enabled: false],
            [name: "drill-on-yarn-maprsasl", enabled: false],
            [name: "yarn-command-insecure", enabled: false],
            [name: "yarn-timelineserver-ui", enabled: false],
            [name: "data-access-gateway-rest-api-pam-ssl" , enabled: false],
            [name: "data-access-gateway-grpc" , enabled: false], //TODO python & node.js
            [name: "kafka-connect-to-maprfs-distributed" , enabled: false],
            [name: "spark-yarn-maprsasl" , enabled: false],
            [name: "spark-standalone" , enabled: false],
            [name: "spark-historyserver-ui", enabled: false],
            [name: "hue-plainauth", enabled: false],
            [name: "sqoop1", enabled: false],
            [name: "sqoop2", enabled: false],
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

            } else if(test['name'] == "mapr-maprcli-api-sasl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                result['mapr-maprcli-api-sasl'] = coreMapRTool.verifyMapRCliApiSasl(packages, ticketfile)

            } else if(test['name'] == "maprlogin-password" && (test['enabled'] as boolean)) {

                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                result['maprlogin-password'] = coreMapRTool.verifyMapRLoginPassword(packages, username, password)

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

            } else if(test['name'] == "yarn-resourcemanager-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("resource_manager_secure_port", DEFAULT_RESOURCEMANAGER_SECURE_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['yarn-resourcemanager-ui-pam-ssl'] = ecoSystemYarn.verifyResourceManagerUIPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "yarn-nodemanager-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("node_manager_secure_port", DEFAULT_NODEMANAGER_SECURE_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['yarn-nodemanager-ui-pam-ssl'] = ecoSystemYarn.verifyNodeManagerUIPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "yarn-resourcemanager-ui-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("resource_manager_insecure_port", DEFAULT_RESOURCEMANAGER_INSECURE_PORT)

                result['yarn-resourcemanager-ui-insecure'] = ecoSystemYarn.verifyRsourceManagerUIInSecure(packages, port)

            } else if(test['name'] == "yarn-nodemanager-ui-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("node_manager_insecure_port", DEFAULT_NODEMANAGER_INSECURE_PORT)

                result['yarn-nodemanager-ui-insecure'] = ecoSystemYarn.verifyNodeManagerUIInSecure(packages, port)

            } else if(test['name'] == "yarn-command-maprsasl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)

                result['yarn-command-maprsasl'] = ecoSystemYarn.verifyYarnCommandMapRSasl(packages, ticketfile)

            } else if(test['name'] == "yarn-historyserver-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("yarn_history_server_secure_port", DEFAULT_YARN_HISTORY_SERVER_SECURE_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['yarn-historyserver-ui-pam-ssl'] = ecoSystemYarn.verifyYarnHistoryServerPamSSL(packages, username, password, certificate, port)

            }  else if(test['name'] == "yarn-historyserver-ui-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("yarn_history_server_insecure_port", DEFAULT_YARN_HISTORY_SERVER_INSECURE_PORT)

                result['yarn-historyserver-ui-insecure'] = ecoSystemYarn.verifyYarnHistoryServerInsecure(packages, port)

            } else if(test['name'] == "hive-server-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_server_ui_port", DEFAULT_HIVE_SERVER_UI_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['hive-server-ui-pam-ssl'] = ecoSystemHive.verifyHiveServerUIPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "hive-client-maprsasl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)

                result['hive-client-maprsasl'] = ecoSystemHive.verifyHiveClientMapRSasl(packages, ticketfile)

            } else if(test['name'] == "hive-beeline-maprsasl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_server_port", DEFAULT_HIVE_SERVER_PORT)
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)

                result['hive-beeline-maprsasl'] = ecoSystemHive.verifyHiveBeelineMapRSasl(packages, ticketfile, port)

            } else if(test['name'] == "hive-beeline-pam" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_server_port", DEFAULT_HIVE_SERVER_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)

                result['hive-beeline-pam'] = ecoSystemHive.verifyHiveBeelinePam(packages, username, password, port)

            } else if(test['name'] == "hive-beeline-maprsasl-pam" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_server_port", DEFAULT_HIVE_SERVER_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)

                result['hive-beeline-maprsasl-pam'] = ecoSystemHive.verifyHiveBeelineMapRSaslPam(packages, ticketfile, username, password, port)

            } else if(test['name'] == "hive-beeline-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_server_port", DEFAULT_HIVE_SERVER_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def truststore = healthcheckconfig.getOrDefault("ssl_truststore_file", DEFAULT_MAPR_SSL_TRUSTSTORE_FILE)

                result['hive-beeline-pam-ssl'] = ecoSystemHive.verifyHiveBeelinePamSSL(packages, truststore, username, password, port)

            } else if(test['name'] == "hive-webhcat-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("hive_webhcat_api_port", DEFAULT_HIVE_WEBHCAT_API_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['hive-webhcat-pam-ssl'] = ecoSystemHive.verifyHiveWebHcatPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "kafka-rest-auth-insecure" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("kafka_rest_port", DEFAULT_KAFKA_REST_PORT)

                result['kafka-rest-auth-insecure'] = ecoSystemKafkaRest.verifyAuthInsecure(packages, port)

            } else if(test['name'] == "kafka-rest-auth-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("kafka_rest_port", DEFAULT_KAFKA_REST_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['kafka-rest-auth-pam-ssl'] = ecoSystemKafkaRest.verifyAuthPamSSL(packages, username, password, certificate, port)

            }  else if(test['name'] == "kafka-rest-api-pam-ssl" && (test['enabled'] as boolean)) {

                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", PATH_TICKET_FILE)
                def port = healthcheckconfig.getOrDefault("kafka_rest_port", DEFAULT_KAFKA_REST_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)

                result['kafka-rest-api-pam-ssl'] = ecoSystemKafkaRest.verifyAPIPamSSL(packages, username, password, certificate, ticketfile, port)

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

                def port = healthcheckconfig.getOrDefault("httpfs_port", DEFAULT_HTTPFS_PORT)

                result['httpfs-auth-insecure'] = ecoSystemHttpfs.verifyAuthInsecure(packages, port)

            } else if(test['name'] == "opentsdb-api" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("opentsdb_api_port", DEFAULT_OPENTSDB_API_PORT)

                result['opentsdb-api'] = ecoSystemSpyglass.verifyOpentsdbAPI(packages, port)

            } else if(test['name'] == "grafana-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def port = healthcheckconfig.getOrDefault("grafana_ui_port", DEFAULT_GRAFANA_UI_PORT)
                def username = healthcheckconfig.getOrDefault("username", DEFAULT_MAPR_USERNAME)
                def password = healthcheckconfig.getOrDefault("password", DEFAULT_MAPR_PASSWORD)
                def certificate = healthcheckconfig.getOrDefault("certificate", PATH_SSL_CERTIFICATE_FILE)
                result['grafana-ui-pam-ssl'] = ecoSystemSpyglass.verifyGrafanaUIPamSSL(packages, username, password, certificate, port)

            } else if(test['name'] == "elasticsearch-healthcheck-pam-ssl" && (test['enabled'] as boolean)) {

                def elastic_port = healthcheckconfig.getOrDefault("grafana_ui_port", DEFAULT_ELASTIC_PORT)
                def username_elastic = healthcheckconfig.getOrDefault("username_elastic", DEFAULT_ELASTIC_USERNAME)
                def password_elastic = healthcheckconfig.getOrDefault("password_elastic", DEFAULT_ELASTIC_PASSWORD)
                def certificate_elastic = healthcheckconfig.getOrDefault("certificate_elastic", PATH_SSL_CERTIFICATE_FILE_ELASTIC)
                result['elasticsearch-healthcheck-pam-ssl'] = ecoSystemSpyglass.verifyElasticPamSSL(packages, username_elastic, password_elastic, certificate_elastic, elastic_port)

            } else if(test['name'] == "kibana-ui-pam-ssl" && (test['enabled'] as boolean)) {

                def kibana_port = healthcheckconfig.getOrDefault("kibana_port", DEFAULT_KIBANA_PORT)
                def username_kibana = healthcheckconfig.getOrDefault("username_kibana", DEFAULT_KIBANA_USERNAME)
                def password_kibana = healthcheckconfig.getOrDefault("password_kibana", DEFAULT_KIBANA_PASSWORD)
                def certificate_kibana = healthcheckconfig.getOrDefault("certificate_kibana", PATH_SSL_CERTIFICATE_FILE_KIBANA)
                result['kibana-ui-pam-ssl'] = ecoSystemSpyglass.verifyKibanaUIPamSSL(packages, username_kibana, password_kibana, certificate_kibana, kibana_port)

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
