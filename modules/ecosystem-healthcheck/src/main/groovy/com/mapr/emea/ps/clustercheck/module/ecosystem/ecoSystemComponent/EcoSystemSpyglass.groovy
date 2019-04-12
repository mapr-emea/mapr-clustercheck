package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
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
     * Verify Grafana UI authentication with PAM
     * @param packages
     * @param credentialFileREST
     * @param certificate
     * @param port
     * @param useSSLCert
     * @return
     */
    def verifyGrafanaUIPam(List<Object> packages, String credentialFileREST, String certificate, int port, Boolean useSSLCert) {

        log.trace("Start : EcoSystemSpyglass : verifyGrafanaUIPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_GRAFANA, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is --netrc-file ${credentialFileREST} ${certToken} https://${remote.host}:${port}/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyGrafanaUIPam")

        testResult
    }

    /**
     * Elastic health check with PAM
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @param useSSLCert
     * @param credentialFileName
     * @return
     */
    def verifyElasticPam(List<Object> packages, String username, String password, String certificate, int port, Boolean useSSLCert, String credentialFileName) {

        log.trace("Start : EcoSystemSpyglass : verifyElasticPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_ELASTIC, {
            def nodeResult = [:]

            final String credentialFileSpyglass = mapRComponentHealthcheckUtil.createCredentialFileSpyglass(credentialFileName, username, password, delegate)

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl --netrc-file ${credentialFileSpyglass} ${certToken} https://${remote.host}:${port}/_cluster/health ; echo \$?"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("cluster_name") && nodeResult['output'].toString().reverse().take(1).equals("0")
            nodeResult['query'] = "sudo " + query

            executeSudo "rm -f ${credentialFileSpyglass}"

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyElasticPam")

        testResult
    }

    /**
     * Verify Kibana UI with Pam
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @param useSSLCert
     * @param credentialFileName
     * @return
     */
    def verifyKibanaUIPam(List<Object> packages, String username, String password, String certificate, int port, Boolean useSSLCert, String credentialFileName) {

        log.trace("Start : EcoSystemSpyglass : verifyKibanaUIPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_KIBANA, {
            def nodeResult = [:]

            final String credentialFileSpyglass = mapRComponentHealthcheckUtil.createCredentialFileSpyglass(credentialFileName, username, password, delegate)

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is --netrc-file ${credentialFileSpyglass} ${certToken} https://${remote.host}:${port}/app/kibana | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            executeSudo "rm -f ${credentialFileSpyglass}"

            nodeResult
        })

        log.trace("End : EcoSystemSpyglass : verifyKibanaUIPam")

        testResult
    }
}
