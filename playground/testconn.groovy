#!/usr/bin/env groovy


// https://gradle-ssh-plugin.github.io/docs/#_connection_settings

@Grab('org.hidetake:groovy-ssh:2.9.0')
@GrabExclude('org.codehaus.groovy:groovy-all')
@Grab('ch.qos.logback:logback-classic:1.1.2')

import groovy.json.JsonOutput

def ssh = org.hidetake.groovy.ssh.Ssh.newService()

ssh.settings {
    knownHosts = allowAnyHosts
//    dryRun = true
}

/*
ssh.remotes.create("node01", {
        role("node")
        host = '10.0.0.24'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    })
*/

ssh.remotes {
    node01 {
        role("node")
        host = '10.0.0.178'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
    node02 {
        role("node")
        host = '10.0.0.201'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
    node03 {
        role("node")
        host = '10.0.0.207'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
}

ssh.run {
    settings {
        pty = true
    }
//    session(ssh.remotes.node01) {
    session(ssh.remotes.role("node")) {
        println(remote.host)
        execute("sleep 5")
        println(remote.host)
    }
}
