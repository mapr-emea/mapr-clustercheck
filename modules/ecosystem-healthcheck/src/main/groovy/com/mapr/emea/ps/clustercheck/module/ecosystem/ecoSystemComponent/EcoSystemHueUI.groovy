package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemHueUI {

    static final Logger log = LoggerFactory.getLogger(EcoSystemHueUI.class)

    static final String PACKAGE_NAME = "mapr-hue"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Hue UI
     * @param packages
     * @param certificate
     * @param port
     * @param useSSLCert
     * @return
     */
    def verifyHueUI(List<Object> packages, String certificate, int port, Boolean useSSLCert){

        log.trace("Start : EcoSystemHttpfs : verifyHueUI")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is ${certToken} https://${remote.host}:${port}/hue/accounts/login/?next=/hue/about/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : EcoSystemHttpfs : verifyHueUI")

        testResult
    }
}
