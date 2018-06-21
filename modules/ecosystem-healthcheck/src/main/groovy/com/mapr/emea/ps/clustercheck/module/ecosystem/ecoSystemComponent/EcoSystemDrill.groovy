package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.EcoSystemHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class EcoSystemDrill {

    static final Logger log = LoggerFactory.getLogger(EcoSystemDrill.class);

    @Autowired
    EcoSystemHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyDrillJdbcPlainAuth(List<Object> packages, String ticketfile, String username, String password, int port) {

        log.info("Start : EcoSystemDrill : verifyDrillJdbcPlainAuth")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile("drill_people.json", delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile("drill_people.sql", delegate)
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} /tmp"
            nodeResult['drillPath'] = execute "ls -d /opt/mapr/drill/drill-*"
            nodeResult['output'] =  executeSudo "${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=PLAIN\" -n ${username} -p ${password} --run=${ sqlPath } --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")
            nodeResult
        })

        log.info("End : EcoSystemDrill : verifyDrillJdbcPlainAuth")

        testResult
    }

    def verifyDrillJdbcMaprSasl(List<Object> packages, String ticketfile, int port) {

        log.info("Start : EcoSystemDrill : verifyDrillJdbcMaprSasl")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, "mapr-drill", {
            def nodeResult = [:]
            def jsonPath = ecoSystemHealthcheckUtil.uploadFile("drill_people.json", delegate)
            def sqlPath = ecoSystemHealthcheckUtil.uploadFile("drill_people.sql", delegate)
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${jsonPath} /tmp"
            nodeResult['drillPath'] = execute "ls -d /opt/mapr/drill/drill-*"
            nodeResult['output'] =  executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} ${nodeResult['drillPath']}/bin/sqlline -u \"jdbc:drill:drillbit=${remote.host}:${port};auth=maprsasl\" --run=${ sqlPath } --force=false --outputformat=csv"
            nodeResult['success'] = nodeResult['output'].contains("Data Engineer")
            nodeResult
        })

        log.info("End : EcoSystemDrill : verifyDrillJdbcMaprSasl")
        testResult
    }
}
