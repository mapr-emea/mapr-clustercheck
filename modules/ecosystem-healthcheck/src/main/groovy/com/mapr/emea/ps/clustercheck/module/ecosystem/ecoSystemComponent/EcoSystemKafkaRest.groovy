package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemKafkaRest {

    static final Logger log = LoggerFactory.getLogger(EcoSystemKafkaRest.class)

    static final String PACKAGE_NAME = "mapr-kafka-rest"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Kafka REST Gateway, REST Client Authentication with SSL and Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyAuthPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemKafkaRest : verifyAuthPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -u ${username}:${password} --cacert ${certificate} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemKafkaRest : verifyAuthPamSSL")

        testResult
    }

    /**
     * Verify Kafka REST Gateway, REST Client in insecure mode
     * @param packages
     * @param port
     * @return
     */
    def verifyAuthInsecure(List<Object> packages, int port) {

        log.trace("Start : EcoSystemKafkaRest : verifyAuthInsecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemKafkaRest : verifyAuthInsecure")

        testResult
    }
}
