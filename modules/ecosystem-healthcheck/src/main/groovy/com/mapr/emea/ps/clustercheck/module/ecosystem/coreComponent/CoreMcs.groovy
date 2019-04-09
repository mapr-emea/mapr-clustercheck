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
     * Verify MCS
     * @param packages
     * @param certificate
     * @param port
     * @return
     */
    def verifyMcsUiSecure(List<Object> packages, String certificate, String credentialFileREST, int port) {

        log.trace("Start : CoreMcs : verifyMcsUiSecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is --netrc-file ${credentialFileREST} --cacert ${certificate} https://${remote.host}:${port}/app/mcs/#/  | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiSecure")
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

            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/app/mcs/#/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : CoreMcs : verifyMcsUiInSecure")
        testResult
    }
}
