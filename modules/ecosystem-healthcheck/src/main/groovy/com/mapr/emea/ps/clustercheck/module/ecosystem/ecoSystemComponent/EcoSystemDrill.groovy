package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemDrill {

    static final Logger log = LoggerFactory.getLogger(EcoSystemDrill.class)

    static final String PACKAGE_NAME = "mapr-drill"
    static final String DIR_MAPR_FS_DRILL = "/tmp/.clustercheck/ecosystem-healthcheck/drill"
    static final String FILE_DRILL_JSON = "drill_people.json"
    static final String FILE_DRILL_QUERY = "drill_people.sql"
    static final String FILE_DRILL_MAPR_DB_QUERY = "drill_people_maprdb.sql"
    static final String TB_DRILL_MAPR_DB_JSON = "drill_people_maprdb"
    static final String PATH_DRILL = "/opt/mapr/drill/drill-*"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a Json File, Using Plain Authentication (PAM)
     * https://mapr.com/docs/home/Drill/connecting_to_a_drillbit.html
     * @param packages
     * @param ticketfile
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyDrillJdbcJsonFilePlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcJsonFilePlainAuth")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]
            def jsonPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_QUERY, delegate)

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS_DRILL}"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

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
            def jsonPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_QUERY, delegate)

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS_DRILL}"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprSasl")
        testResult
    }

    /**
     * Verify Drill JDBC Driver, Connecting to a Drillbit, Querying a MapR-DB Table, Using MapR SASL
     * @param packages
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyDrillJdbcMaprdbJsonMaprSasl(List<Object> packages, String ticketfile, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]
            def jsonPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_MAPR_DB_QUERY, delegate)

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"

            def jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(ticketfile, jsonPath, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${DIR_MAPR_FS_DRILL}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv; echo \$?"
            nodeResult['success'] = nodeResult['output'].contains("4 rows selected") && nodeResult['output'].toString().reverse().take(1).equals("0")

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")
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
    def verifyDrillJdbcMaprdbJsonPlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]
            def jsonPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = mapRComponentHealthcheckUtil.uploadFile(FILE_DRILL_MAPR_DB_QUERY, delegate)

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"

            def jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(ticketfile, jsonPath, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${DIR_MAPR_FS_DRILL}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${sqlPath} --force=false --outputformat=csv; echo \$?"
            nodeResult['success'] = nodeResult['output'].contains("4 rows selected") && nodeResult['output'].toString().reverse().take(1).equals("0")

            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")
        testResult
    }

    /**
     *  Verifying Drill web UI, secure (SSL) mode
     * @param packages
     * @param certificate
     * @param port
     */
    def verifyDrillUISecureSSL(List<Object> packages, String certificate, int port) {
        log.trace("Start : EcoSystemDrill : verifyDrillUISecureSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is --cacert ${certificate} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUISecureSSL")

        testResult
    }


    /**
     * Verifying Drill web UI, secured mode (PAM)
     * @param packages
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyDrillUISecurePAM(List<Object> packages, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillUISecurePAM")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -k -u ${username}:${password} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
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
            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUIInsecure")

        testResult
    }

}
