@Grab('com.aestasit.infrastructure.sshoogr:sshoogr:0.9.25')

import static com.aestasit.infrastructure.ssh.DefaultSsh.*

remoteSession {
    trustUnknownHosts = true
    user = 'ec2-user'
    host = '10.0.0.24'
    keyFile = new File("/Users/chufe/.ssh/id_rsa")
    connect()

    exec 'hostname -f'
//    remoteFile('/var/my.conf').text = "enabled=true"
}
