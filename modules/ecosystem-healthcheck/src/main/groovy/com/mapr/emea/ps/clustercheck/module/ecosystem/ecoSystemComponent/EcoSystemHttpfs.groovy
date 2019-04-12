package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemHttpfs {

    static final Logger log = LoggerFactory.getLogger(EcoSystemHttpfs.class)

    static final String PACKAGE_NAME = "mapr-httpfs"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     *  Verify HTTPFS Authentication with Pam (Pam is mandatory)
     * @param packages
     * @param certificate
     * @param credentialFileREST
     * @param useSSLCert
     * @param port
     * @return
     */
    def verifyAuthPam(List<Object> packages, String certificate, String credentialFileREST, Boolean useSSLCert, int port) {

        log.trace("Start : EcoSystemHttpfs : verifyAuthPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"

            final String query = "curl -Is --netrc-file ${credentialFileREST} ${certToken} \"https://${remote.host}:${port}/webhdfs/v1/?op=GETHOMEDIRECTORY\" | head -n 1"
            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : EcoSystemHttpfs : verifyAuthPam")

        testResult

    }

    /**
     * Verify HTTPFS Authentication in Insecure mode
     * @param packages
     * @param port
     * @return
     */
    def verifyAuthInsecure(List<Object> packages, int port) {

        log.trace("Start : EcoSystemHttpfs : verifyAuthInsecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is \"http://mapr:mapr@${remote.host}:${port}/webhdfs/v1/?op=GETHOMEDIRECTORY\" | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemHttpfs : verifyAuthInsecure")

        testResult

    }
}
