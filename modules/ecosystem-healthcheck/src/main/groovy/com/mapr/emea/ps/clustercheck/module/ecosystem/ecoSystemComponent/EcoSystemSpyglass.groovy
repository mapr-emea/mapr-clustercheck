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

            nodeResult['output'] = executeSudo "curl http://${remote.host}:${port}/api/version | grep -q version; echo \$?"
            nodeResult['success'] = nodeResult['output'].toString().reverse().take(1).equals("0")

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

            nodeResult['output'] = executeSudo "curl -Is --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyGrafanaUIPamSSL")

        testResult
    }
}
