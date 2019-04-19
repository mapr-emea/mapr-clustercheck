package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class EcoSystemHive {

    static final Logger log = LoggerFactory.getLogger(EcoSystemHive.class)

    static final String PACKAGE_NAME_HIVE_SERVER2 = "mapr-hiveserver2"
    static final String PACKAGE_NAME_HIVE_CLIENT = "mapr-hive-"
    static final String PACKAGE_NAME_HIVE_WEBHCAT = "mapr-hivewebhcat"

    @Autowired
    @Qualifier("localTmpDir")
    String tmpPath

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Hive Server UI, Authentication with Pam (Pam is mandatory)
     * @param packages
     * @param credentialFileREST
     * @param useSSLCert
     * @param port
     * @return
     */
    def verifyHiveServerUIPam(List<Object> packages, String certificate, String credentialFileREST, Boolean useSSLCert, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveServerUIPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_SERVER2, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is --netrc-file ${credentialFileREST} ${certToken} https://${remote.host}:${port}/hiveserver2.jsp | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = query

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveServerUIPam")

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

                final String query = "MAPR_TICKETFILE_LOCATION=${ticketfile} hive -e \"show databases;\"; echo \$?"

                nodeResult['output']  = executeSudo query
                nodeResult['success'] = nodeResult['output'].contains("OK") && nodeResult['output'].toString().reverse().take(1).equals("0")
                nodeResult['query']   = query

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

                final String query = "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;auth=maprsasl;\" -e \"show databases;\"; echo \$?"

                nodeResult['HiveServer-' + it]            = [:]
                nodeResult['HiveServer-' + it]['output']  = executeSudo query
                nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
                nodeResult['HiveServer-' + it]['query']   = query
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
    def verifyHiveBeelinePam(List<Object> packages, String username, String password, String credentialFileName, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelinePam")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            final String credentialFilePath = "${tmpPath}/${credentialFileName}"

            try {

                executeSudo "rm -f ${credentialFilePath}"
                executeSudo "echo ${password} >> ${credentialFilePath}"
                executeSudo "chmod 400 ${credentialFilePath}"

                hiveServerHosts.each {

                    final String query = "/opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;user=${username};password=`cat ${credentialFilePath}`\" -e \"show databases;\"; echo \$?"

                    nodeResult['HiveServer-' + it]            = [:]
                    nodeResult['HiveServer-' + it]['output']  = executeSudo query
                    nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
                    nodeResult['HiveServer-' + it]['query']   = query
                }

            } catch (Exception e) {
                throw e
            } finally {
                executeSudo "rm -f ${credentialFilePath}"
                log.debug("Local password credential file was Purged successfully.")
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
    def verifyHiveBeelineMapRSaslPam(List<Object> packages, String ticketfile, String username, String password, String credentialFileName, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelineMapRSaslPam")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            final String credentialFilePath = "${tmpPath}/${credentialFileName}"

            try {

                executeSudo "rm -f ${credentialFilePath}"
                executeSudo "echo ${password} >> ${credentialFilePath}"
                executeSudo "chmod 400 ${credentialFilePath}"

                hiveServerHosts.each {

                    final String query = "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;auth=maprsasl;user=${username};password=`cat ${credentialFilePath}`\" -e \"show databases;\"; echo \$?"

                    nodeResult['HiveServer-' + it]            = [:]
                    nodeResult['HiveServer-' + it]['output']  = executeSudo query
                    nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
                    nodeResult['HiveServer-' + it]['query']   = query
                }

            } catch (Exception e) {
                throw e
            } finally {
                executeSudo "rm -f ${credentialFilePath}"
                log.debug("Local password credential file was Purged successfully.")
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
    def verifyHiveBeelinePamSSL(List<Object> packages, String truststore, String username, String password, String credentialFileName, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveBeelinePamSSL")

        def hiveServerHosts = mapRComponentHealthcheckUtil.findHostsWithPackage(packages, PACKAGE_NAME_HIVE_SERVER2)

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_CLIENT, {
            def nodeResult = [:]

            final String credentialFilePath = "${tmpPath}/${credentialFileName}"

            try {

                executeSudo "rm -f ${credentialFilePath}"
                executeSudo "echo ${password} >> ${credentialFilePath}"
                executeSudo "chmod 400 ${credentialFilePath}"

                hiveServerHosts.each {

                    final String query = "/opt/mapr/hive/hive-*/bin/beeline -u \"jdbc:hive2://${it}:${port}/default;ssl=true;sslTrustStore=${truststore};user=${username};password=`cat ${credentialFilePath}`\" -e \"show databases;\"; echo \$?"

                    nodeResult['HiveServer-' + it]            = [:]
                    nodeResult['HiveServer-' + it]['output']  = execute query
                    nodeResult['HiveServer-' + it]['success'] = nodeResult['HiveServer-' + it]['output'].contains("Connected to: Apache Hive") && nodeResult['HiveServer-' + it]['output'].toString().reverse().take(1).equals("0")
                    nodeResult['HiveServer-' + it]['query']   = query

                }

            } catch (Exception e) {
                throw e
            } finally {
                executeSudo "rm -f ${credentialFilePath}"
                log.debug("Local password credential file was Purged successfully.")
            }

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveBeelinePamSSL")

        testResult
    }

    /**
     * Verify Hive WebHcat API, Authentication with Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyHiveWebHcatPam(List<Object> packages, String username, String credentialFileREST, int port) {

        log.trace("Start : EcoSystemHive : verifyHiveWebHcatPam")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME_HIVE_WEBHCAT, {
            def nodeResult = [:]

            final String query = "curl -Is --netrc-file ${credentialFileREST} http://${remote.host}:${port}/templeton/v1/status?user.name=${username} | head -n 1"

            nodeResult['output']  = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['comment'] = "Only one WebHcat is running, the others are standby."
            nodeResult['query']   = query

            nodeResult
        })

        log.trace("End : EcoSystemHive : verifyHiveWebHcatPam")

        testResult
    }

}
