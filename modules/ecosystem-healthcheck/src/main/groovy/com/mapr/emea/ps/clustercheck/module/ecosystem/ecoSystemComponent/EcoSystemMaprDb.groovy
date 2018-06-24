package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemMaprDb {

    static final Logger log = LoggerFactory.getLogger(EcoSystemMaprDb.class)

    static final String DIR_MAPR_FS = "/tmp/.clustercheck/ecosystem-healthcheck/maprdb"
    static final String FILE_MAPR_DB_JSON_QUERY = "maprdb_json_query"
    static final String FILE_MAPR_DB_JSON = "maprdb_people.json"
    static final String TB_MAPR_DB_JSON = "tb_maprdb_people"
    static final String TB_MAPR_DB_BINARY = "test_binary"
    static final String CF_MAPR_DB_BINARY = "cfname_test_binary"

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyMaprdbJsonShell(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbJsonShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-core", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile("maprdb_people.json", delegate)
            def queryPath = ecoSystemHealthcheckUtil.uploadFile(FILE_MAPR_DB_JSON_QUERY, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${DIR_MAPR_FS}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${DIR_MAPR_FS}/${FILE_MAPR_DB_JSON} -dst ${DIR_MAPR_FS}/${TB_MAPR_DB_JSON} -mapreduce false"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr dbshell script ${queryPath}"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            nodeResult
        })

        log.trace("End : EcoSystemMaprDb : verifyMaprdbJsonShell")
        testResult
    }

    def verifyMaprdbBinaryShell(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbBinaryShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-core", {
            def nodeResult = [:]

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table create -path ${DIR_MAPR_FS}/${TB_MAPR_DB_BINARY}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf create -path ${DIR_MAPR_FS}/${TB_MAPR_DB_BINARY} -cfname ${CF_MAPR_DB_BINARY}"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf list -path ${DIR_MAPR_FS}/${TB_MAPR_DB_BINARY}"
            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.cluster.root failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."
            nodeResult['success'] = nodeResult['output'].contains(CF_MAPR_DB_BINARY)

            nodeResult
        })

        log.trace("End : EcoSystemMaprDb : verifyMaprdbBinaryShell")
        testResult
    }

}
