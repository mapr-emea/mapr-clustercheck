package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemHive {

    static final Logger log = LoggerFactory.getLogger(EcoSystemHive.class)

    static final String PACKAGE_NAME_HIVE_SERVER2 = "mapr-hiveserver2"
    static final String PACKAGE_NAME_HIVE_CLIENT = "mapr-hive-"
    static final String PACKAGE_NAME_HIVE_WEBHCAT = "mapr-hivewebhcat"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Hive Server UI, Authentication with SSL and Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyHiveServerUIPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveServerUIPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_SERVER2, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/hiveserver2.jsp | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveServerUIPamSSL")

        testResult
    }

    /**
     * Verify Hive Server / Metastore connection using Hive client with MapR-SASL
     * @param packages
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyHiveClientMapRSasl(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemHive : verifyHiveClientMapRSasl")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hive -e \"show databases;\"; echo \$?"
            nodeResult['success'] = nodeResult['output'].contains("OK") && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveClientMapRSasl")

        testResult
    }

    /**
     * Verify Hive Server / Metastore connection using Beeline client with MapR-SASL
     * @param packages
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyHiveBeelineMapRSasl(List<Object> packages, String ticketfile, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelineMapRSasl")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            hiveServerHosts.each {
                nodeResult['HiveServer-' + it] = [:]
                nodeResult['HiveServer-' + it]['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;auth=maprsasl;\" -e \"show databases;\"; echo \$?"
                nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveBeelineMapRSasl")

        testResult
    }

    /**
     * Verify Hive Server / Metastore connection using Beeline client with PAM
     * @param packages
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyHiveBeelinePam(List<Object> packages, String username, String password, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelinePam")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            hiveServerHosts.each {
                nodeResult['HiveServer-' + it] = [:]
                nodeResult['HiveServer-' + it]['output'] = executeSudo "/opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;user=${username};password=${password}\" -e \"show databases;\"; echo \$?"
                nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveBeelinePam")

        testResult
    }

    /**
     * Verify Hive Server / Metastore connection using Beeline client with MapRSASL and PAM
     * @param packages
     * @param ticketfile
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyHiveBeelineMapRSaslPam(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelineMapRSaslPam")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            hiveServerHosts.each {
                nodeResult['HiveServer-' + it] = [:]
                nodeResult['HiveServer-' + it]['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;auth=maprsasl;user=${username};password=${password}\" -e \"show databases;\"; echo \$?"
                nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveBeelineMapRSaslPam")

        testResult
    }

    /**
     * Verify Hive Server / Metastore connection using Beeline client with PAM and SSL
     * @param packages
     * @param truststore
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyHiveBeelinePamSSL(List<Object> packages, String truststore, String username, String password, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelinePamSSL")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            hiveServerHosts.each {
                nodeResult['HiveServer-' + it] = [:]
                nodeResult['HiveServer-' + it]['output'] = execute "/opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;ssl=true;sslTrustStore=${truststore};user=${username};password=${password}\" -e \"show databases;\"; echo \$?"
                nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveBeelinePamSSL")

        testResult
    }

    /**
     * Verify Hive WebHcat API, Authentication with SSL and Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyHiveWebHcatPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveWebHcatPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_WEBHCAT, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is --cacert ${certificate} -u ${username}:${password} https://${remote.host}:${port}/templeton/v1/status?user.name=${username} | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult['comment'] = "Only one WebHcat is running, the others are standby."
            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveWebHcatPamSSL")

        testResult
    }

}
