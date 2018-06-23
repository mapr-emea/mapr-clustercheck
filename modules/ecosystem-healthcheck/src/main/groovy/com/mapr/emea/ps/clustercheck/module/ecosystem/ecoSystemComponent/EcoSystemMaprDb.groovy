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

    def verifyMaprdbJsonShell(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbJsonShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-core", {
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

        log.trace("End : EcoSystemMaprDb : verifyMaprdbJsonShell")
        testResult
    }

    def verifyMaprdbBinaryShell(List<Object> packages, String ticketfile) {

        log.trace("Start : EcoSystemMaprDb : verifyMaprdbBinaryShell")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-core", {
            def nodeResult = [:]

            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${maprFsDir}"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table create -path ${maprFsDir}/test_binary"
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf create -path ${maprFsDir}/test_binary -cfname cfname_test_binary"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli table cf list -path ${maprFsDir}/test_binary"
            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.cluster.root failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."
            nodeResult['success'] = nodeResult['output'].contains("cfname_test_binary")

            nodeResult
        })

        log.trace("End : EcoSystemMaprDb : verifyMaprdbBinaryShell")
        testResult
    }

}
