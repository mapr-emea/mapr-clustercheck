package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMcs {

    static final Logger log = LoggerFactory.getLogger(CoreMcs.class)

    static final String PACKAGE_NAME = "mapr-webserver"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify MCS in Secure cluster without authentication
     * @param packages
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyMcsUiSecure(List<Object> packages, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiSecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is -k https://${remote.host}:${port}/app/mcs/#/  | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiSecure")
        testResult
    }

    /**
     * Verify MCS, Secure (SSL) Mode
     * @param packages
     * @param certificate
     * @param port
     * @return
     */
    def verifyMcsUiSecureSSL(List<Object> packages, String certificate, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiSecureSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is --cacert ${certificate} https://${remote.host}:${port}/app/mcs/#/  | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query
            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiSecureSSL")
        testResult
    }

    /**
     * Verify MCS rest api with PAM
     * @param packages
     * @param credentialFileREST
     * @param port
     * @return
     */
    def verifyMcsApiSecurePAM(List<Object> packages, String credentialFileREST, int port) {

        log.trace("Start : CoreMcs : verifyMcsApiSecurePAM")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is -k --netrc-file ${credentialFileREST} https://${remote.host}:${port}/rest/node/list  | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsApiSecurePAM")
        testResult
    }

    /**
     * Verify MCS rest api with PAM + SSL
     * @param packages
     * @param certificate
     * @param credentialFileREST
     * @param port
     * @return
     */
    def verifyMcsApiSecurePAMSSL(List<Object> packages, String credentialFileREST, String certificate, int port) {

        log.trace("Start : CoreMcs : verifyMcsApiSecurePAMSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is -k --netrc-file ${credentialFileREST} --cacert ${certificate} https://${remote.host}:${port}/rest/node/list  | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsApiSecurePAMSSL")
        testResult
    }

    /**
     * Verify MCS, Insecure Mode
     * @param packages
     * @param port
     * @return
     */
    def verifyMcsUiInSecure(List<Object> packages, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiInSecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is http://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['output'] = executeSudo
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiInSecure")
        testResult
    }
}
