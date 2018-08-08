package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemDrill {

    static final Logger log = LoggerFactory.getLogger(EcoSystemDrill.class)

    static final String DIR_MAPR_FS_DRILL = "/tmp/.clustercheck/ecosystem-healthcheck/drill"
    static final String FILE_DRILL_JSON = "drill_people.json"
    static final String FILE_DRILL_QUERY = "drill_people.sql"
    static final String FILE_DRILL_MAPR_DB_QUERY = "drill_people_maprdb.sql"
    static final String TB_DRILL_MAPR_DB_JSON = "drill_people_maprdb"
    static final String PATH_DRILL = "/opt/mapr/drill/drill-*"
    
    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyDrillJdbcPlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcPlainAuth")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_QUERY, delegate)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS_DRILL}"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcPlainAuth")

        testResult
    }

    def verifyDrillJdbcMaprSasl(List<Object> packages, String ticketfile, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprSasl")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_QUERY, delegate)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS_DRILL}"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprSasl")
        testResult
    }

    def verifyDrillJdbcMaprdbJsonPlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_MAPR_DB_QUERY, delegate)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"

            def jsonPathMaprfs = ecoSystemHealthcheckUtil.uploadRemoteFileToMaprfs(ticketfile, jsonPath, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${DIR_MAPR_FS_DRILL}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("4 rows selected")

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonPlainAuth")
        testResult
    }

    def verifyDrillJdbcMaprdbJsonMaprSasl(List<Object> packages, String ticketfile, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_JSON, delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile(FILE_DRILL_MAPR_DB_QUERY, delegate)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_DRILL}"

            def jsonPathMaprfs = ecoSystemHealthcheckUtil.uploadRemoteFileToMaprfs(ticketfile, jsonPath, DIR_MAPR_FS_DRILL, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${DIR_MAPR_FS_DRILL}/${TB_DRILL_MAPR_DB_JSON} -mapreduce false"
            nodeResult['drillPath'] = execute "ls -d ${PATH_DRILL}"
            nodeResult['output'] =  executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${sqlPath} --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("4 rows selected")

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_DRILL, delegate)

            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillJdbcMaprdbJsonMaprSasl")
        testResult
    }

    def verifyDrillUiUnsecured(List<Object> packages, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillUiUnsecured")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            nodeResult['output'] = executeSudo "curl -Is http://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUiUnsecured")
        testResult
    }

    def verifyDrillUiSecured(List<Object> packages, String username, String password, int port) {

        log.trace("Start : EcoSystemDrill : verifyDrillUiSecured")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]

            nodeResult['output'] = executeSudo "curl -Is -k -u ${username}:${password} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['success'] = nodeResult['output'].contains("HTTP/1.1 200 OK")
            nodeResult
        })

        log.trace("End : EcoSystemDrill : verifyDrillUiSecured")
        testResult
    }
}
