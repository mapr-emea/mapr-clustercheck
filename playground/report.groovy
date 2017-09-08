#!/usr/bin/env groovy


def jsonSlurper = new groovy.json.JsonSlurper()
def object = jsonSlurper.parse(new File("/Users/chufe/Documents/workspaces/daimler-review/dxc/audit/2017-09-06-17-18-43/modules/benchmark-maprfs-rwtest/result.json"))

object.moduleResult.each { result ->
    println "Node: ${result.host} - Disks: ${result.numberOfDisks} - Write: ${result.sumWriteRateInMBperSecond} MB/s - Read: ${result.sumReadRateInMBperSecond} MB/s"
}

//