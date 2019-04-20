package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
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
    @Qualifier("maprFSTmpDir")
    String maprFSTmpDir

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify MapRDB Json, Using mapr dbshell utility
     * https://mapr.com/docs/home/MapR-DB/JSON_DB/getting_started_json_ojai_using_maprdb_shell.html
     * @param packages
     * @param ticketfile
     *  @return
     */
    def verifyMapRDBJsonShell(List<Object> packages, String ticketfile, Boolean purgeaftercheck) {

        log.trace("Start : CoreMapRDB : verifyMapRDBJsonShell")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            final String jsonPath = mapRComponentHealthcheckUtil.uploadResourceFileToRemoteHost(DIR_MAPRDB, FILE_MAPR_DB_JSON, delegate)
            final String jsonPathMaprfs = mapRComponentHealthcheckUtil.uploadRemoteFileToMaprfs(DIR_MAPRDB, ticketfile, jsonPath, delegate)
            final String pathTableMapRFS = "${jsonPathMaprfs}/${TB_MAPR_DB_JSON}"

            final String queryImportJson = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${jsonPathMaprfs} -dst ${pathTableMapRFS} -mapreduce false"
            final String queryDbShell = "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr dbshell find ${pathTableMapRFS}; echo \$?"

            executeSudo queryImportJson

            nodeResult['output'] = executeSudo queryDbShell
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer") && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1-path-after-uploading-json-remote-host'] = jsonPath
            nodeResult['2-path-after-sending-json-maprfs']        = jsonPathMaprfs
            nodeResult['3-query-import-json-maprdb']              = "sudo " + queryImportJson
            nodeResult['4-query-dbshel']                          = "sudo " + queryDbShell

            if(purgeaftercheck){
                final String queryPurge = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r -f ${jsonPathMaprfs}; echo \$?"
                nodeResult['5-query-purge']                       = "sudo " + queryPurge
                nodeResult['purge_output'] = executeSudo queryPurge
                nodeResult['purge_success'] = nodeResult['purge_output'].toString().reverse().take(1).equals("0")
            }

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
    def verifyMapRDBBinaryShell(List<Object> packages, String ticketfile, Boolean purgeaftercheck) {

        log.trace("Start : CoreMapRDB : verifyMapRDBBinaryShell")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            final String pathTableMapRFS = "${maprFSTmpDir}/${DIR_MAPRDB}"
            final String queryCreateDir = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir ${pathTableMapRFS}"
            final String queryCreateTable = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table create -path ${pathTableMapRFS}/${TB_MAPR_DB_BINARY}"
            final String queryCreateCF = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf create -path ${pathTableMapRFS}/${TB_MAPR_DB_BINARY} -cfname ${CF_MAPR_DB_BINARY}"
            final String queryListCF = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf list -path ${pathTableMapRFS}/${TB_MAPR_DB_BINARY}; echo \$?"

            executeSudo queryCreateDir
            executeSudo queryCreateTable
            executeSudo queryCreateCF

            nodeResult['output'] = executeSudo queryListCF
            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.cluster.root failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."
            nodeResult['success'] = nodeResult['output'].contains(CF_MAPR_DB_BINARY) && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1-query-create-dir']           = "sudo " + queryCreateDir
            nodeResult['2-query-create-table']         = "sudo " + queryCreateTable
            nodeResult['3-query-create-column-family'] = "sudo " + queryCreateCF
            nodeResult['4-query-list-column-family']   = "sudo " + queryListCF

            if(purgeaftercheck){
                final String queryPurge = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r -f ${pathTableMapRFS}; echo \$?"
                nodeResult['5-query-purge']            = "sudo " + queryPurge
                nodeResult['purge_output'] = executeSudo queryPurge
                nodeResult['purge_success'] = nodeResult['purge_output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : CoreMapRDB : verifyMapRDBBinaryShell")
        testResult
    }

}
