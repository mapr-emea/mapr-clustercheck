package com.mapr.emea.ps.clustercheck.module.ecosystem.ecoSystemComponent

import com.mapr.emea.ps.clustercheck.module.ecosystem.util.MapRComponentHealthcheckUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class EcoSystemKafkaRest {

    static final Logger log = LoggerFactory.getLogger(EcoSystemKafkaRest.class)

    static final String PACKAGE_NAME = "mapr-kafka-rest"

    static final String DIR_KAFKA_REST = "kafkarest"

    static final String STREAM_NAME = "test_stream1"

    static final String TOPIC_NAME = "test_stream1_topic1"

    static final String CONSUMER_NAME = "test_stream1_topic1_consumer1"

    @Autowired
    @Qualifier("maprFSTmpDir")
    String tmpMapRPath

    @Autowired
    MapRComponentHealthcheckUtil mapRComponentHealthcheckUtil

    /**
     * Verify Kafka REST Gateway, REST Client Authentication with SSL and Pam (Pam is mandatory)
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param port
     * @return
     */
    def verifyAuthPamSSL(List<Object> packages, String username, String password, String certificate, int port) {

        log.trace("Start : EcoSystemKafkaRest : verifyAuthPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is -u ${username}:${password} --cacert ${certificate} https://${remote.host}:${port}/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemKafkaRest : verifyAuthPamSSL")

        testResult
    }

    /**
     * Verify Kafka REST Gateway, REST Client in insecure mode
     * @param packages
     * @param port
     * @return
     */
    def verifyAuthInsecure(List<Object> packages, int port) {

        log.trace("Start : EcoSystemKafkaRest : verifyAuthInsecure")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String query = "curl -Is http://${remote.host}:${port}/ | head -n 1"

            nodeResult['output'] = executeSudo query
            nodeResult['success'] = nodeResult['output'].toString().contains("HTTP/1.1 200 OK")
            nodeResult['query'] = "sudo " + query

            nodeResult
        })

        log.trace("End : EcoSystemKafkaRest : verifyAuthInsecure")

        testResult
    }

    /**
     * Test API with Pan and SSL
     * 1. Create path, stream and topic
     * 2. Create consumer
     * 3. Produce a Json message
     * 4. Consume the message
     * 5. Clean up the path, stream, topic and the consumer
     * @param packages
     * @param username
     * @param password
     * @param certificate
     * @param ticketfile
     * @param port
     * @return
     */
    def verifyAPIPamSSL(List<Object> packages, String username, String password, String certificate, String ticketfile, int port) {

        log.trace("Start : EcoSystemKafkaRest : verifyAPIPamSSL")

        def testResult = mapRComponentHealthcheckUtil.executeSsh(packages, PACKAGE_NAME, {
            def nodeResult = [:]

            final String path                 = "${tmpMapRPath}/${DIR_KAFKA_REST}"
            final String pathHTML             = path.replace("/","%2F")
            final String messages             = "'{\"records\":[{\"value\": {\"user_id\" : 1234567, \"name\" : \"paul\", \"age\" : 4} }, {\"value\": {\"user_id\" : 12345678, \"name\" : \"paul\", \"age\" : 5} }, {\"value\": {\"user_id\" : 123456789, \"name\" : \"paul\", \"age\" : 6} }]}'"
            final String queryCreateDir       = "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${path}"
            final String queryCreateStream    = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream create -path ${path}/${STREAM_NAME} -produceperm p -consumeperm p -topicperm p"
            final String queryCreateTopic     = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream topic create -path ${path}/${STREAM_NAME} -topic ${TOPIC_NAME}"
            final String queryCreateConsumer  = "curl -u ${username}:${password} --cacert ${certificate} -X POST -H \"Content-Type: application/vnd.kafka.v1+json\" --data '{\"name\": \"${CONSUMER_NAME}\", \"format\": \"json\", \"auto.offset.reset\": \"earliest\"}' https://${remote.host}:${port}/consumers/${pathHTML}%2F${STREAM_NAME}%3A${TOPIC_NAME}"
            final String queryProduceMessage  = "MAPR_TICKETFILE_LOCATION=${ticketfile} /opt/mapr/kafka/kafka-*/bin/kafka-console-producer.sh --broker-list this.will.beignored:9092 --topic /${path}/${STREAM_NAME}:${TOPIC_NAME} <<< \$${messages}"
            final String queryConsumeMessage  = "curl -u ${username}:${password} --cacert ${certificate} -X GET -H \"Accept: application/vnd.kafka.json.v1+json\" https://${remote.host}:${port}/consumers/${pathHTML}%2F${STREAM_NAME}%3A${TOPIC_NAME}/instances/${CONSUMER_NAME}/topics/${pathHTML}%2F${STREAM_NAME}%3A${TOPIC_NAME}"
            final String queryDeleteConsumer  = "curl -u ${username}:${password} --cacert ${certificate} -X DELETE https://${remote.host}:${port}/consumers/${pathHTML}%2F${STREAM_NAME}%3A${TOPIC_NAME}/instances/${CONSUMER_NAME}"
            final String queryDeleteStream    = "MAPR_TICKETFILE_LOCATION=${ticketfile} maprcli stream delete -path ${path}/${STREAM_NAME}"

            //Create a test directory
            executeSudo queryCreateDir

            //Create a test stream
            executeSudo queryCreateStream

            //Create a test topic in the stream
            executeSudo queryCreateTopic

            //Create a consumer
            executeSudo queryCreateConsumer

            //Produce a message
            executeSudo queryProduceMessage

            executeSudo queryConsumeMessage //TODO why 2 times? offset?

            //Consume the message
            nodeResult['output'] = executeSudo queryConsumeMessage
            nodeResult['success'] = nodeResult['output'].contains("paul")

            nodeResult['1.  Query : create the Dir     '] = "sudo " + queryCreateDir
            nodeResult['2.  Query : create the Stream  '] = "sudo " + queryCreateStream
            nodeResult['3.  Query : create the Topic   '] = "sudo " + queryCreateTopic
            nodeResult['4.  Query : create the Consumer'] = "sudo " + queryCreateConsumer
            nodeResult['5.  Query : produce a Message  '] = "sudo " + queryProduceMessage
            nodeResult['6.  Query : consume the Message'] = "sudo " + queryConsumeMessage
            nodeResult['7.  Query : delete the Consumer'] = "sudo " + queryDeleteConsumer
            nodeResult['8.  Query : delete the Stream  '] = "sudo " + queryDeleteStream

            //Clean up

            //Delete the consumer
            executeSudo queryDeleteConsumer

            //Delete the test stream
            executeSudo queryDeleteStream

            sleep(5000)

            nodeResult
        })

        log.trace("End : EcoSystemKafkaRest : verifyAPIPamSSL")

        testResult
    }
}
