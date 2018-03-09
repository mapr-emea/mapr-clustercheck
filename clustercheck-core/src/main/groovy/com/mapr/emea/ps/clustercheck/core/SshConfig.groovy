package com.mapr.emea.ps.clustercheck.core

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component

/**
 * Created by chufe on 22.08.17.
 * See https://gradle-ssh-plugin.github.io/docs/
 */
@Configuration
class SshConfig {
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Bean("ssh")
    def sshConfig() {

        def ssh = org.hidetake.groovy.ssh.Ssh.newService()

        ssh.settings {
            knownHosts = allowAnyHosts
//    dryRun = true
        }
        def nodes = globalYamlConfig.get("nodes")
        for (Map<String, Object> node  : nodes) {
            ssh.remotes.create(node.host, {
                role(node.host)
                role('all')
                for (String roleName  : node.roles) {
                    role(roleName)
                }
                host = node.get('host', 'localhost')
                user = node.ssh_user
                port = node.get('ssh_port', 22)
                identity = new File(node.ssh_identity)
            })
        }
        return ssh;

    }
}
