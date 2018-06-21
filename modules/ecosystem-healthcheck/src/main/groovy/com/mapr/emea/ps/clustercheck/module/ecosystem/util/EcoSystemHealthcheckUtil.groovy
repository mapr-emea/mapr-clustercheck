package com.mapr.emea.ps.clustercheck.module.ecosystem.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader
import org.springframework.stereotype.Component

@Component
class EcoSystemHealthcheckUtil {

    static final Logger log = LoggerFactory.getLogger(EcoSystemHealthcheckUtil.class)

    @Autowired
    @Qualifier("ssh")
    def ssh

    @Autowired
    ResourceLoader resourceLoader

    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c '${exec}'"
    }

    def retrievePackages(role) {

        log.trace("Start : EcoSystemHealthcheckUtil : retrievePackages")

        def packages = Collections.synchronizedList([])
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
                node['host'] = remote.host
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                if (distribution.toLowerCase().contains("ubuntu")) {
                    node['mapr.packages'] = executeSudo('apt list --installed | grep mapr').tokenize('\n')
                } else {
                    node['mapr.packages'] = executeSudo('rpm -qa | grep mapr').tokenize('\n')
                }
                packages.add(node)
            }
        }

        log.trace("End : EcoSystemHealthcheckUtil : retrievePackages")
        return packages
    }

    def static List<Object> findHostsWithPackage(List packages, packageName) {
        log.trace("Start : EcoSystemHealthcheckUtil : findHostsWithPackage")
        def hostsFound = packages.findAll { it['mapr.packages'].find { it.contains(packageName) } != null }.collect { it['host'] }

        log.trace("End : EcoSystemHealthcheckUtil : findHostsWithPackage")

        return hostsFound
    }

    def executeSsh(List<Object> packages, String packageName, Closure closure) {
        log.trace("Start : EcoSystemHealthcheckUtil : executeSsh")

        def appHosts = findHostsWithPackage(packages, packageName)
        def result = Collections.synchronizedList([])
        appHosts.each { appHost ->
            log.info(">>>>>>> ..... testing node ${appHost}")
            ssh.runInOrder {
                settings {
                    pty = true
                    ignoreError = true
                }
                session(ssh.remotes.role(appHost)) {
                    def node = [:]
                    node['host'] = remote.host
                    closure.delegate = delegate
                    node += closure()
                    result.add(node)
                }
            }
        }

        log.trace("End : EcoSystemHealthcheckUtil : executeSsh")
        result
    }

    def uploadFile(String fileName, delegate) {
        log.trace("Start : EcoSystemHealthcheckUtil : uploadFile")

        def homePath = delegate.execute 'echo $HOME'
        delegate.execute "mkdir -p ${homePath}/.clustercheck/ecosystem-healthcheck/"
        def fileInputStream = resourceLoader.getResource("classpath:/com/mapr/emea/ps/clustercheck/module/ecosystem/healthcheck/${fileName}").getInputStream()
        delegate.put from: fileInputStream, into: "${homePath}/.clustercheck/ecosystem-healthcheck/${fileName}"
        def path = "${homePath}/.clustercheck/ecosystem-healthcheck/${fileName}"

        log.trace("End : EcoSystemHealthcheckUtil : uploadFile")

        return path
    }
}
