package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
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
    def verifyCldbPamSASL(List<Object> packages, String username, String password, String ticketfile, int port) {

        log.trace("Start : CoreCLDB : verifyCldbPamSASL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String fqdn = execute "hostname -f"
            final String queryCLDBMaster = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli node cldbmaster"

            //Cert is not working
            nodeResult['output'] = executeSudo "curl -Is -k -u ${username}:${password} https://${remote.host}:${port}/ | head -n 1"
            nodeResult['ui_success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['cldbmaster'] = queryCLDBMaster.contains(fqdn)

            nodeResult
        })

        log.trace("End : CoreCLDB : verifyCldbPamSASL")
        testResult
    }
}
