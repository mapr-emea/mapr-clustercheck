package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMapRDB {

    static final Logger log = LoggerFactory.getLogger(CoreMapRDB.class)

    static final String PACKAGE_NAME = "mapr-core"
    static final String DIR_MAPR_FS_MAPRDB = "/tmp/.clustercheck/ecosystem-healthcheck/maprdb"
    static final String FILE_MAPR_DB_JSON_QUERY = "maprdb_json_query"
    static final String FILE_MAPR_DB_JSON = "maprdb_people.json"
    static final String TB_MAPR_DB_JSON = "tb_maprdb_people"
    static final String TB_MAPR_DB_BINARY = "test_binary"
    static final String CF_MAPR_DB_BINARY = "cfname_test_binary"

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    /**
     * Verify MapRDB Json, Using mapr dbshell utility
     * https://mapr.com/docs/home/MapR-DB/JSON_DB/getting_started_json_ojai_using_maprdb_shell.html
     * @param packages
     * @param ticketfile
     * @return
     */
    def verifyMapRDBJsonShell(List<Object> packages, String ticketfile) {

        log.trace("Start : CoreMapRDB : verifyMapRDBJsonShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile(FILE_MAPR_DB_JSON, delegate)
            def queryPath = ecoSystemHealthcheckUtil.uploadFile(FILE_MAPR_DB_JSON_QUERY, delegate)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_MAPRDB, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_MAPRDB}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS_MAPRDB}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${DIR_MAPR_FS_MAPRDB}/${FILE_MAPR_DB_JSON} -dst ${DIR_MAPR_FS_MAPRDB}/${TB_MAPR_DB_JSON} -mapreduce false"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr dbshell script ${queryPath}"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_MAPRDB, delegate)

            nodeResult
        })

        log.trace("End : CoreMapRDB : verifyMapRDBJsonShell")
        testResult
    }

    /**
     * Verify MapRDB Binary, Using maprcli utility
     * @param packages
     * @param ticketfile
     * @return
     */
    def verifyMapRDBBinaryShell(List<Object> packages, String ticketfile) {

        log.trace("Start : CoreMapRDB : verifyMapRDBBinaryShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_MAPRDB, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_MAPRDB}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table create -path ${DIR_MAPR_FS_MAPRDB}/${TB_MAPR_DB_BINARY}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf create -path ${DIR_MAPR_FS_MAPRDB}/${TB_MAPR_DB_BINARY} -cfname ${CF_MAPR_DB_BINARY}"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf list -path ${DIR_MAPR_FS_MAPRDB}/${TB_MAPR_DB_BINARY}"
            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.cluster.root failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."
            nodeResult['success'] = nodeResult['output'].contains(CF_MAPR_DB_BINARY)

            ecoSystemHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_MAPRDB, delegate)

            nodeResult
        })

        log.trace("End : CoreMapRDB : verifyMapRDBBinaryShell")
        testResult
    }

}
