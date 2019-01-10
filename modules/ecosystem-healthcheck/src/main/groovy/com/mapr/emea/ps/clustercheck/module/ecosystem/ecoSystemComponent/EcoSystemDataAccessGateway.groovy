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
     * Verify MapR Data Access Gateway, REST Client Authentication with SSL and Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyRESTAuthPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemDataAccessGateway : verifyRESTAuthPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -u ${username}:${password} --cacert ${certificate} https://${remote.host}:${port}/app/swagger/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDataAccessGateway : verifyRESTAuthPamSSL")

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
