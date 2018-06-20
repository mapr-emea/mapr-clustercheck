package com.mapr.emea.ps.clustercheck.module.ecosystem.healthcheck

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
@ClusterCheckModule(name = "ecosystem-healthcheck", version = "1.0")
class EcoSystemHealthcheckModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(EcoSystemHealthcheckModule.class);

    @Autowired
    @Qualifier("ssh")
    def ssh
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    ResourceLoader resourceLoader;

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
        def packages = retrievePackages(role)
        healthcheckconfig['tests'].each { test ->
            log.info(">>>>>>> Running test '${test['name']}'")
            if(test['name'] == "drill-jdbc-file-json-plainauth" && (test['enabled'] as boolean)) {
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", "/opt/mapr/conf/mapruserticket")
                def username = healthcheckconfig.getOrDefault("username", "mapr")
                def password = healthcheckconfig.getOrDefault("password", "mapr")
                def port = healthcheckconfig.getOrDefault("drill_port", 31010)
                result['drill-jdbc-file-json-plainauth'] = verifyDrillJdbcPlainAuth(packages, ticketfile, username, password, port)
            } else if(test['name'] == "drill-jdbc-file-json-maprsasl" && (test['enabled'] as boolean)) {
                def ticketfile = healthcheckconfig.getOrDefault("ticketfile", "/opt/mapr/conf/mapruserticket")
                def port = healthcheckconfig.getOrDefault("drill_port", 31010)
                result['drill-jdbc-file-json-maprsasl'] = verifyDrillJdbcMaprSasl(packages, ticketfile, port)
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

    def verifyDrillJdbcPlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {
        def testResult = executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = uploadFile("drill_people.json", delegate)
            def sqlPath = uploadFile("drill_people.sql", delegate)
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} /tmp"
            nodeResult['drillPath'] = execute "ls -d /opt/mapr/drill/drill-*"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=localhost:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${ sqlPath } --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['queryOutput'].contains("Data Engineer")
            nodeResult
        })
        testResult
    }

    def verifyDrillJdbcMaprSasl(List<Object> packages, String ticketfile, int port) {
        def testResult = executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = uploadFile("drill_people.json", delegate)
            def sqlPath = uploadFile("drill_people.sql", delegate)
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} /tmp"
            nodeResult['drillPath'] = execute "ls -d /opt/mapr/drill/drill-*"
            nodeResult['queryOutput'] =  executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=localhost:${port};auth=maprsasl\" --run=${ sqlPath } --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['queryOutput'].contains("Data Engineer")
            nodeResult
        })
        testResult
    }

    def uploadFile(String fileName, delegate) {
        def homePath = delegate.execute 'echo $HOME'
        delegate.execute "mkdir -p ${homePath}/.clustercheck/ecosystem-healthcheck/"
        def fileInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/ecosystem/healthcheck/${fileName}").getInputStream()
        delegate.put from: fileInputStream, into: "${homePath}/.clustercheck/ecosystem-healthcheck/${fileName}"
        return "${homePath}/.clustercheck/ecosystem-healthcheck/${fileName}"
    }


    def executeSsh(List<Object> packages, String packageName, Closure closure) {
        def appHosts = findHostsWithPackage(packages, packageName)
        def result = Collections.synchronizedList([])
        appHosts.each { appHost ->
            log.info(">>>>>>> ..... testing node ${appHost}")
            ssh.runInOrder {
                settings {
                    pty = true
                    ignoreError = true
                }
                session(ssh.remotes.role(appHost)) {
                    def node = [:]
                    node['host'] = remote.host
                    closure.delegate = delegate
                    node += closure()
                    result.add(node)
                }
            }
        }
        result
    }

    def List<Object> findHostsWithPackage(List packages, packageName) {
        packages.findAll { it['mapr.packages'].find { it.contains(packageName) } != null }.collect { it['host'] }
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

    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c '${exec}'"
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
