package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CoreMapRStreams {

    static final Logger log = LoggerFactory.getLogger(CoreMapRStreams.class)

    static final String PACKAGE_NAME = "mapr-core"

    static final String DIR_MAPR_FS_MAPRSTREAMS = "/tmp/.clustercheck/ecosystem-healthcheck/maprstreams"

    static final String STREAM_NAME = "test_mapr_stream"

    static final String TOPIC_NAME = "test_mapr_stream_topic1"

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil


    def verifyMapRStreams(List<Object> packages, String ticketfile) {

        log.trace("Start : CoreMapRStreams : verifyMapRStreams")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            //Create a test directory
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${DIR_MAPR_FS_MAPRSTREAMS}"

            //Create a test stream
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream create -path ${DIR_MAPR_FS_MAPRSTREAMS}/${STREAM_NAME}"

            //Create a test topic in the stream
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream topic create -path ${DIR_MAPR_FS_MAPRSTREAMS}/${STREAM_NAME} -topic ${TOPIC_NAME}"

            nodeResult['output'] = executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream topic list -path ${DIR_MAPR_FS_MAPRSTREAMS}/${STREAM_NAME} "

            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.** failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."

            nodeResult['success'] = nodeResult['output'].contains(TOPIC_NAME)

            //Delete the test stream
            executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream delete -path ${DIR_MAPR_FS_MAPRSTREAMS}/${STREAM_NAME}"

            //Delete the test directory
            mapRComponentHealthcheckUtil.removeMaprfsFileIfExist(ticketfile, DIR_MAPR_FS_MAPRSTREAMS, delegate)

            nodeResult
        })

        log.trace("End : CoreMapRStreams : verifyMapRStreams")
        testResult
    }
}
