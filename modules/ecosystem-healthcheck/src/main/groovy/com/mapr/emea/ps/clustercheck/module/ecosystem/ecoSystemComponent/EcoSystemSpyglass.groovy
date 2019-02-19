package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemSpyglass {

    static final Logger log = LoggerFactory.getLogger(EcoSystemSpyglass.class)

    static final String PACKAGE_NAME_OPENTSDB = "mapr-opentsdb"
    static final String PACKAGE_NAME_GRAFANA = "mapr-grafana"
    static final String PACKAGE_NAME_ELASTIC = "mapr-elasticsearch"
    static final String PACKAGE_NAME_KIBANA = "mapr-kibana"
    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Opentsdb API Connection
     * @param packages
     * @param port
     * @return
     */
    def verifyOpentsdbAPI(List<Object> packages, int port) {

        log.trace("Start : EcoSystemSpyglass : verifyOpentsdbAPI")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_OPENTSDB, {
            def nodeResult = [:]

            final String query = "curl http://${remote.host}:${port}/api/version | grep -q version; echo \$?"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().reverse().take(1).equals("0")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyOpentsdbAPI")

        testResult
    }

    /**
     * Verify Grafana UI authentication with PAM and SSL
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyGrafanaUIPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemSpyglass : verifyGrafanaUIPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_GRAFANA, {
            def nodeResult = [:]

            final String query = "curl -Is --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyGrafanaUIPamSSL")

        testResult
    }

    /**
     * Elastic health check with PAM and SSL
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyElasticPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemSpyglass : verifyElasticPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_ELASTIC, {
            def nodeResult = [:]

            final String query = "curl --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/_cluster/health ; echo \$?"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("cluster_name") && nodeResult['output'].toString().reverse().take(1).equals("0")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyElasticPamSSL")

        testResult
    }

    /**
     * Verify Kibana UI with Pam and SSL
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyKibanaUIPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemSpyglass : verifyKibanaPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_KIBANA, {
            def nodeResult = [:]

            final String query = "curl -Is --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/app/kibana | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyKibanaPamSSL")

        testResult
    }
}
