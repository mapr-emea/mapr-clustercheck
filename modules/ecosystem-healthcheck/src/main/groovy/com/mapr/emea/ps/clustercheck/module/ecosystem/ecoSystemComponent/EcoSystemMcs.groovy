package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemMcs {

    static final Logger log = LoggerFactory.getLogger(EcoSystemMcs.class)

    static final String PACKAGE_NAME = "mapr-webserver"

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyMcsUiSecure(List<Object> packages, String username, String password, int port) {

        log.trace("Start : EcoSystemMcs : verifyMcsUiSecure")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -k -u ${username}:${password} https://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemMcs : verifyMcsUiSecure")
        testResult
    }

    def verifyMcsUiInSecureSSL(List<Object> packages, int port) {

        log.trace("Start : EcoSystemMcs : verifyMcsUiInSecureSSL")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -k https://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemMcs : verifyMcsUiInSecureSSL")
        testResult
    }
}
