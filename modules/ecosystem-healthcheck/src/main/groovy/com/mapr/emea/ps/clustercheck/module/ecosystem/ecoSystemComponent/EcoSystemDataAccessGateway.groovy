package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemDataAccessGateway {

    static final Logger log = LoggerFactory.getLogger(EcoSystemDataAccessGateway.class)

    static final String PACKAGE_NAME = "mapr-data-access-gateway"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify MapR Data Access Gateway, REST Client Authentication with Pam (Pam is mandatory)
     * @param packages
     * @param certificate
     * @param credentialFileREST
     * @param useSSLCert
     * @param port
     * @return
     */
    def verifyRESTAuthPam(List<Object> packages, String certificate, String credentialFileREST, Boolean useSSLCert, int port) {

        log.trace("Start : EcoSystemDataAccessGateway : verifyRESTAuthPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is --netrc-file ${credentialFileREST} ${certToken} https://${remote.host}:${port}/app/swagger/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : EcoSystemDataAccessGateway : verifyRESTAuthPam")

        testResult

    }

    /**
     * Verify MapR Data Access Gateway, REST Client in insecure mode
     * @param packages
     * @param port
     * @return
     */
    def verifyRESTAuthInsecure(List<Object> packages, int port) {

        log.trace("Start : EcoSystemDataAccessGateway : verifyRESTAuthInsecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/app/swagger/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDataAccessGateway : verifyRESTAuthInsecure")

        testResult
    }
}
