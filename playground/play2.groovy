#!/usr/bin/env groovy

@Grab('org.hidetake:groovy-ssh:2.9.0')
@GrabExclude('org.codehaus.groovy:groovy-all')
@Grab('ch.qos.logback:logback-classic:1.1.2')
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
        host = '10.0.0.24'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
    node02 {
        role("node")
        role("zookeeper")
        host = '10.0.0.167'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
    node03 {
        role("node")
        host = '10.0.0.239'
        user = 'ec2-user'
        identity = new File('/Users/chufe/.ssh/id_rsa')
    }
}

def collect = new StringBuilder()
ssh.run {
    settings {
        pty = true
    }
//    session(ssh.remotes.node01) {
    session(ssh.remotes.role('zookeeper')) {
      execute 'hostname -f'
//      def output = execute 'hostname -f'
//      collect.append(output).append('\n')
    }
}

println(collect)