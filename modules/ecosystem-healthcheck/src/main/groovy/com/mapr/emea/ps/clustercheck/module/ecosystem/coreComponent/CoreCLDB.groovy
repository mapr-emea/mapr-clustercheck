package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class CoreCLDB {

    static final Logger log = LoggerFactory.getLogger(CoreCLDB.class)

    static final String PACKAGE_NAME = "mapr-cldb"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil


    /**
     * Verify CLDB with PAM
     * @param packages
     * @param username
     * @param password
     * @param port
     * @return
     */
    def verifyCldbPamSASL(List<Object> packages, String credentialFileREST, String ticketfile, int port) {

        log.trace("Start : CoreCLDB : verifyCldbPamSASL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String fqdn = execute "hostname -f"
            final String queryCLDBMaster = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli node cldbmaster"
            final String queryCldbUI = "curl -Is -k --netrc-file ${credentialFileREST} https://${remote.host}:${port}/ | head -n 1"

            final String cldbMaster = executeSudo queryCLDBMaster

            nodeResult['ui-output'] = executeSudo queryCldbUI
            nodeResult['ui-success'] = nodeResult['ui-output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['cldb-master'] = cldbMaster.contains(fqdn)
            nodeResult['1-query-cldb-ui'] = queryCldbUI
            nodeResult['2-query-cldbmaster'] = "sudo " + queryCLDBMaster


            nodeResult
        })

        log.trace("End : CoreCLDB : verifyCldbPamSASL")
        testResult
    }
}
