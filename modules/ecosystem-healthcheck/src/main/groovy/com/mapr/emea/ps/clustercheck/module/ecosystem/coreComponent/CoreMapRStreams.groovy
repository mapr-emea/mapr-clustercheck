package com.mapr.emea.ps.clustercheck.module.ecosystem.coreComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class CoreMapRStreams {

    static final Logger log = LoggerFactory.getLogger(CoreMapRStreams.class)

    static final String PACKAGE_NAME = "mapr-core"

    static final String DIR_MAPR_FS_MAPRSTREAMS = "maprstreams"

    static final String STREAM_NAME = "test_mapr_stream"

    static final String TOPIC_NAME = "test_mapr_stream_topic1"

    @Autowired
    @Qualifier("maprFSTmpDir")
    String maprFSTmpDir

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify MapR Streams, create a topic in a stream and list topics
     * @param packages
     * @param ticketfile
     * @param maprFSTmpDir
     * @return
     */
    def verifyMapRStreams(List<Object> packages, String ticketfile, Boolean purgeaftercheck) {

        log.trace("Start : CoreMapRStreams : verifyMapRStreams")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {

            def nodeResult = [:]

            final String pathStreamMapRFS = "${maprFSTmpDir}/${DIR_MAPR_FS_MAPRSTREAMS}"
            final String queryCreateDir = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${pathStreamMapRFS}"
            final String queryCreateStream = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream create -path ${pathStreamMapRFS}/${STREAM_NAME}"
            final String queryCreateTopic = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream topic create -path ${pathStreamMapRFS}/${STREAM_NAME} -topic ${TOPIC_NAME}"
            final String queryListTopic = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream topic list -path ${pathStreamMapRFS}/${STREAM_NAME}; echo \$?"

            executeSudo queryCreateDir
            executeSudo queryCreateStream
            executeSudo queryCreateTopic

            nodeResult['output'] = executeSudo queryListTopic

            nodeResult['comment'] = "Don't worry if you see the error: Lookup of volume mapr.** failed, error Read-only file system(30) ... it just means in clusters.conf the read-only CLDB is first tried then redirected to the read/write CLDB server."

            nodeResult['success'] = nodeResult['output'].contains(TOPIC_NAME) && nodeResult['output'].toString().reverse().take(1).equals("0")

            nodeResult['1-query-create-dir']    = "sudo " + queryCreateDir
            nodeResult['2-query-create-stream'] = "sudo " + queryCreateStream
            nodeResult['3-query-create-topic']  = "sudo " + queryCreateTopic
            nodeResult['4-query-list-topic']    = "sudo " + queryListTopic

            if(purgeaftercheck){
                final String queryPurge = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r -f ${pathStreamMapRFS}; echo \$?"
                nodeResult['5-query-purge']                       = "sudo " + queryPurge
                nodeResult['purge_output'] = executeSudo queryPurge
                nodeResult['purge_success'] = nodeResult['purge_output'].toString().reverse().take(1).equals("0")
            }

            nodeResult
        })

        log.trace("End : CoreMapRStreams : verifyMapRStreams")
        testResult
    }
}
