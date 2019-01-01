package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMfs {
    static final Logger log = LoggerFactory.getLogger(CoreMfs.class)

    static final String PACKAGE_NAME = "mapr-core"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    def verifyMaprFs(List<Object> packages, String ticketfile) {

        log.trace("Start : CoreMfs : verifyMaprFs")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]
            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -ls /user"
            nodeResult['success'] = nodeResult['output'].contains("mapr")

            nodeResult
        })

        log.trace("End : CoreMfs : verifyMaprFs")

        testResult
    }
}
