package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMapRLogin {

    static final Logger log = LoggerFactory.getLogger(CoreMapRLogin.class)

    static final String PACKAGE_NAME = "mapr-core"

    @Autowired
    MapRComponentHealthcheckUtil ecoSystemHealthcheckUtil

    def verifyMapRLoginPassword(List<Object> packages, String username, String password) {
        log.trace("Start : CoreMapRLogin : verifyMapRLoginPassword")

        def testResult = ecoSystemHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            def uid = executeSudo("su - ${username} -c 'id -u ${username}'")

            def ticketFile = "/tmp/maprticket_${uid}"

            nodeResult['output'] = executeSudo("su - ${username} -c 'echo ${password} | maprlogin password'")

            nodeResult['success'] = nodeResult['output'].contains(ticketFile)
            nodeResult
        })

        log.trace("End : CoreMapRLogin : verifyMapRLoginPassword")

        testResult
    }
}
