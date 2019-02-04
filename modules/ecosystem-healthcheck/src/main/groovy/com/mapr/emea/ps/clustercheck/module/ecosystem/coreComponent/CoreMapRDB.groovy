package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMapRDB {

    static final Logger log = LoggerFactory.getLogger(CoreMapRDB.class)

    static final String PACKAGE_NAME = "mapr-core"
    static final String DIR_MAPRDB = "maprdb"
    static final String FILE_MAPR_DB_JSON = "maprdb_people.json"
    static final String TB_MAPR_DB_JSON = "tb_maprdb_people"
    static final String TB_MAPR_DB_BINARY = "test_binary"
    static final String CF_MAPR_DB_BINARY = "cfname_test_binary"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify MapRDB Json, Using mapr dbshell utility
     * https://mapr.com/docs/home/MapR-DB/JSON_DB/getting_started_json_ojai_using_maprdb_shell.html
     * @param packages
     * @param ticketfile
     * @param maprFSTmpDir
     *  @return
     */
    def verifyMapRDBJsonShell(List<Object> packages, String ticketfile, String maprFSTmpDir) {

        log.trace("Start : CoreMapRDB : verifyMapRDBJsonShell")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            final String jsonPath = mapRComponentHealthcheckUtil.uploadFileToRemoteHost(DIR_MAPRDB, FILE_MAPR_DB_JSON, delegate)
            final String query = "find " + "${maprFSTmpDir}/${DIR_MAPRDB}/${TB_MAPR_DB_JSON}"
            final String queryCreateDir = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir ${maprFSTmpDir}/${DIR_MAPRDB}"
            final String querySendJsonFile = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${maprFSTmpDir}/${DIR_MAPRDB}"
            final String queryImportJson = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${maprFSTmpDir}/${DIR_MAPRDB}/${FILE_MAPR_DB_JSON} -dst ${maprFSTmpDir}/${DIR_MAPRDB}/${TB_MAPR_DB_JSON} -mapreduce false"
            final String queryDbShell = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr dbshell ${query}; echo \$?"

            executeSudo queryCreateDir
            executeSudo querySendJsonFile
            executeSudo queryImportJson

            nodeResult['output'] = executeSudo queryDbShell
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer") && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1. Path  : after uploading json file to remote host '] = jsonPath
            nodeResult['2. Query : find table                               '] = query
            nodeResult['3. Query : create dir                               '] = "sudo " + queryCreateDir
            nodeResult['4. Query : send json file to MapRFS                 '] = "sudo " + querySendJsonFile
            nodeResult['5. Query : import json file to MapRDB               '] = "sudo " + queryImportJson
            nodeResult['6. Query : execute query                            '] = "sudo " + queryDbShell

            nodeResult
        })

        log.trace("End : CoreMapRDB : verifyMapRDBJsonShell")
        testResult
    }

    /**
     * Verify MapRDB Binary, Using maprcli utility
     * @param packages
     * @param ticketfile
     * @param maprFSTmpDir
     * @return
     */
    def verifyMapRDBBinaryShell(List<Object> packages, String ticketfile, maprFSTmpDir) {

        log.trace("Start : CoreMapRDB : verifyMapRDBBinaryShell")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            final String queryCreateDir = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir ${maprFSTmpDir}/${DIR_MAPRDB}"
            final String queryCreateTable = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table create -path ${maprFSTmpDir}/${DIR_MAPRDB}/${TB_MAPR_DB_BINARY}"
            final String queryCreateCF = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf create -path ${maprFSTmpDir}/${DIR_MAPRDB}/${TB_MAPR_DB_BINARY} -cfname ${CF_MAPR_DB_BINARY}"
            final String queryListCF = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf list -path ${maprFSTmpDir}/${DIR_MAPRDB}/${TB_MAPR_DB_BINARY}; echo \$?"

            executeSudo queryCreateDir
            executeSudo queryCreateTable
            executeSudo queryCreateCF

            nodeResult['output'] = executeSudo queryListCF
            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.cluster.root failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."
            nodeResult['success'] = nodeResult['output'].contains(CF_MAPR_DB_BINARY) && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1. Query : create dir           '] = "sudo " + queryCreateDir
            nodeResult['2. Query : create table         '] = "sudo " + queryCreateTable
            nodeResult['3. Query : create column family '] = "sudo " + queryCreateCF
            nodeResult['4. Query : list column family   '] = "sudo " + queryListCF
            nodeResult['5. Query : delete dir           '] = "No query available"

            nodeResult
        })

        log.trace("End : CoreMapRDB : verifyMapRDBBinaryShell")
        testResult
    }

}
