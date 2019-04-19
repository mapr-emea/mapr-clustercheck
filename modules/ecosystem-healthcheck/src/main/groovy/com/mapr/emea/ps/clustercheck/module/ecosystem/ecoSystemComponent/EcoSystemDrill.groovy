package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import com.sun.org.apache.xpath.internal.operations.Bool
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class EcoSystemDrill {

    static final Logger log = LoggerFactory.getLogger(EcoSystemDrill.class)

    static final String PACKAGE_NAME = "mapr-drill"
    static final String DIR_DRILL = "drill"
    static final String FILE_DRILL_JSON = "drill_people.json"
    static final String FILE_DRILL_QUERY = "drill_people.sql"
    static final String FILE_DRILL_MAPR_DB_QUERY = "drill_people_maprdb.sql"
    static final String TB_DRILL_MAPR_DB_JSON = "drill_people_maprdb"

    @Autowired
    @Qualifier("localTmpDir")
    String tmpPath

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a Json File, Using Plain Authentication (PAM) TODO
     * https://mapr.com/docs/home/Drill/connecting_to_a_drillbit.html
     * @param packages
     * @param ticketfile
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyDrillJdbcJsonFilePlainAuth(List<Object> packages, String ticketfile, String username, String password, String credentialFileName, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcJsonFilePlainAuth")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String credentialFilePath = "${tmpPath}/${credentialFileName}"

            try {
                executeSudo "rm -f ${credentialFilePath}"
                executeSudo "echo ${password} >> ${credentialFilePath}"
                executeSudo "chmod 400 ${credentialFilePath}"

                final String jsonPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_JSON, delegate)
                final String sqlPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_QUERY, delegate)
                final String jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(DIR_DRILL, ticketfile, jsonPath, delegate)

                final String queryExecution = "/opt/mapr/drill/drill-*/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p `cat ${credentialFilePath}` --run=${sqlPath} --force=false --outputformat=csv"

                nodeResult['output'] =  executeSudo queryExecution
                nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

                nodeResult['1. Path  : after uploading json file to remote host  '] = jsonPath
                nodeResult['2. Path  : after uploading query file to remote host '] = sqlPath
                nodeResult['3. Path  : after sending json file to MapRFS         '] = jsonPathMaprfs
                nodeResult['4. Query : execute query                             '] = "sudo " + queryExecution

            } catch (Exception e) {
                throw e
            } finally {
                executeSudo "rm -f ${credentialFilePath}"
                log.debug("Local password credential file was Purged successfully.")
            }

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcJsonFilePlainAuth")

        testResult
    }

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a Json File, Using MapR SASL
     * https://mapr.com/docs/home/Drill/connecting_to_a_drillbit.html
     * @param packages
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyDrillJdbcMaprSasl(List<Object> packages, String ticketfile, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprSasl")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String jsonPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_JSON, delegate)
            final String sqlPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_QUERY, delegate)
            final String jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(DIR_DRILL, ticketfile, jsonPath, delegate)

            final String queryExecution = "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/drill/drill-*/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv"

            nodeResult['output'] =  executeSudo queryExecution
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            nodeResult['1. Path  : after uploading json file to remote host  '] = jsonPath
            nodeResult['2. Path  : after uploading query file to remote host '] = sqlPath
            nodeResult['3. Query : after sending json file to MapRFS         '] = jsonPathMaprfs
            nodeResult['4. Query : execute query                             '] = "sudo " + queryExecution

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprSasl")
        testResult
    }

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a MapR-DB Table, Using Plain Authentication (PAM)
     * @param packages
     * @param ticketfile
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyDrillJdbcMaprdbJsonPlainAuth(List<Object> packages, String ticketfile, String username, String password, String credentialFileName, int port, Boolean purgeaftercheck) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String credentialFilePath = "${tmpPath}/${credentialFileName}"

            try {

                executeSudo "rm -f ${credentialFilePath}"
                executeSudo "echo ${password} >> ${credentialFilePath}"
                executeSudo "chmod 400 ${credentialFilePath}"

                final String jsonPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_JSON, delegate)
                final String sqlPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_MAPR_DB_QUERY, delegate)
                final String jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(DIR_DRILL, ticketfile, jsonPath, delegate)

                final String queryImportJson = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${jsonPathMaprfs}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
                final String queryExecution = "/opt/mapr/drill/drill-*/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p `cat ${credentialFilePath}` --run=${sqlPath} --force=false --outputformat=csv; echo \$?"

                executeSudo queryImportJson

                nodeResult['output'] =  executeSudo queryExecution
                nodeResult['success'] = nodeResult['output'].contains("4 rows selected") && nodeResult['output'].toString().reverse().take(1).equals("0")

                nodeResult['1. Path  : after uploading json file to remote host   '] = jsonPath
                nodeResult['2. Path  : after uploading query file to remote host  '] = sqlPath
                nodeResult['3. Path  : after sending json file to MapRFS          '] = jsonPathMaprfs
                nodeResult['4. Query : import json file to MapRDB                 '] = "sudo " + queryImportJson
                nodeResult['5. Query : execute query                              '] = "sudo " + queryExecution

                if(purgeaftercheck){
                    final String queryPurge = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r -f ${jsonPathMaprfs}; echo \$?"
                    nodeResult['6-query-purge']                       = "sudo " + queryPurge
                    nodeResult['purge_output'] = executeSudo queryPurge
                    nodeResult['purge_success'] = nodeResult['purge_output'].toString().reverse().take(1).equals("0")
                }

            } catch (Exception e) {
                throw e
            } finally {
                executeSudo "rm -f ${credentialFilePath}"
                log.debug("Local password credential file was Purged successfully.")
            }

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")
        testResult
    }

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a MapR-DB Table, Using MapR SASL
     * @param packages
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyDrillJdbcMaprdbJsonMaprSasl(List<Object> packages, String ticketfile, int port, Boolean purgeaftercheck) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String jsonPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_JSON, delegate)
            final String sqlPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_DRILL, FILE_DRILL_MAPR_DB_QUERY, delegate)
            final String jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(DIR_DRILL, ticketfile, jsonPath, delegate)

            final String queryImportJson = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${jsonPathMaprfs}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
            final String queryExecution = "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/drill/drill-*/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv; echo \$?"

            executeSudo queryImportJson

            nodeResult['output'] = executeSudo queryExecution
            nodeResult['success'] = nodeResult['output'].contains("4 rows selected") && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1. Path  : after uploading json file to remote host       '] = jsonPath
            nodeResult['2. Path  : after uploading query file to remote host      '] = sqlPath
            nodeResult['3. Query : after sending json file to MapRFS              '] = jsonPathMaprfs
            nodeResult['4. Query : import json file to MapRDB                     '] = "sudo " + queryImportJson
            nodeResult['5. Query : execute query                                  '] = "sudo " + queryExecution

            if(purgeaftercheck){
                final String queryPurge = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r -f ${jsonPathMaprfs}; echo \$?"
                nodeResult['6-query-purge']                       = "sudo " + queryPurge
                nodeResult['purge_output'] = executeSudo queryPurge
                nodeResult['purge_success'] = nodeResult['purge_output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")
        testResult
    }

    /**
     * Verifying Drill web UI
     * @param packages
     * @param certificate
     * @param port
     * @param useSSLCert
     * @return
     */
    def verifyDrillUISecure(List<Object> packages, String certificate, int port, Boolean useSSLCert) {

        log.trace("Start : EcoSystemDrill : verifyDrillUISecurePAM")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String certToken = (useSSLCert == true) ? "--cacert ${certificate}" : "-k"
            final String query = "curl -Is ${certToken} https://${remote.host}:${port}/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUISecurePAM")

        testResult
    }

    /**
     * Verifying Drill web UI, insecure mode
     * @param packages
     * @param port
     * @return
     */
    def verifyDrillUIInsecure(List<Object> packages, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillUIInsecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is http://${remote.host}:${port}/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUIInsecure")

        testResult
    }

}
