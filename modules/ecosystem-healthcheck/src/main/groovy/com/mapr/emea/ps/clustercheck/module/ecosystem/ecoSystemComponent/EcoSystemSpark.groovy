package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemSpark {

    static final Logger log = LoggerFactory.getLogger(EcoSystemSpark.class)

    static final String PACKAGE_NAME_SPARK_HISTORYSERVER = "mapr-spark-historyserver"
    static final String PACKAGE_NAME_SPARK = "mapr-spark"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Spark historyserver UI with PAM
     * @param packages
     * @param certificate
     * @param credentialFileREST
     * @param port
     * @param useSSLCert
     * @return
     */
    def verifyHistoryServerUIPAM(List<Object> packages, String certificate, String credentialFileREST, int port, Boolean useSSLCert) {
        log.trace("Start : EcoSystemSpark : verifyHistoryServerUIPAM")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_SPARK_HISTORYSERVER, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is --netrc-file ${credentialFileREST} ${certToken} https://${remote.host}:${port} | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })


        log.trace("Start : EcoSystemSpark : verifyHistoryServerUIPAM")

        testResult
    }
}
