package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemMaprDb {

    static final Logger log = LoggerFactory.getLogger(EcoSystemMaprDb.class)

    static final String maprFsDir = "/tmp/.clustercheck/ecosystem-healthcheck/maprdb"
    static final String maprdbJsonQueryFile = "maprdb_json_query"
    static final String maprdbJsonFile = "maprdb_people.json"
    static final String maprdbJsonTable = "tb_maprdb_people"

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyMaprdbJsonShellMaprSasl(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbJsonShellMaprSasl")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile("maprdb_people.json", delegate)
            def queryPath = ecoSystemHealthcheckUtil.uploadFile(maprdbJsonQueryFile, delegate)

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${maprFsDir}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} ${maprFsDir}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr importJSON -idField name -src ${maprFsDir}/${maprdbJsonFile} -dst ${maprFsDir}/${maprdbJsonTable} -mapreduce false"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} mapr dbshell script ${queryPath}"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            nodeResult
        })

        log.trace("End : EcoSystemMaprDb : verifyMaprdbJsonShellMaprSasl")
        testResult
    }

    def verifyMaprdbJsonShellUnsecured(List<Object> packages) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbJsonShellUnsecured")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile("maprdb_people.json", delegate)
            def queryPath = ecoSystemHealthcheckUtil.uploadFile(maprdbJsonQueryFile, delegate)

            executeSudo "hadoop fs -mkdir -p ${maprFsDir}"
            executeSudo "hadoop fs -put -f ${jsonPath} ${maprFsDir}"
            executeSudo "mapr importJSON -idField name -src ${maprFsDir}/${maprdbJsonFile} -dst ${maprFsDir}/${maprdbJsonTable} -mapreduce false"

            nodeResult['output'] = executeSudo "mapr dbshell script ${queryPath}"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")

            nodeResult
        })

        log.trace("End : EcoSystemMaprDb : verifyMaprdbJsonShellUnsecured")
        testResult
    }
}
