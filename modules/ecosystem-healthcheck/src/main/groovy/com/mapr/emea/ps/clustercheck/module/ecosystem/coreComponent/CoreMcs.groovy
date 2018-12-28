package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMcs {

    static final Logger log = LoggerFactory.getLogger(CoreMcs.class)

    static final String PACKAGE_NAME = "mapr-webserver"

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyMcsUiSecurePAM(List<Object> packages, String username, String password, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiSecurePAM")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -k -u ${username}:${password} https://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiSecurePAM")
        testResult
    }

    def verifyMcsUiSecureSSL(List<Object> packages, String certificate, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiSecureSSL")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is --cacert ${certificate} https://${remote.host}:${port}/app/mcs/#/  | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiSecureSSL")
        testResult
    }

    def verifyMcsUiInSecure(List<Object> packages, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiInSecure")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiInSecure")
        testResult
    }
}
