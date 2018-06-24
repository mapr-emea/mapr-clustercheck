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

    static final String PATH_ECO_SYS = ".clustercheck/ecosystem-healthcheck"

    static final String PATH_CLASSPATH = "/com/mapr/emea/ps/clustercheck/module/ecosystem/healthcheck"

    @Autowired
    @Qualifier("ssh")
    def ssh

    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Autowired
    ResourceLoader resourceLoader

    def suStr(String ticketFile, exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=${ticketFile};${exec}'"
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

    static List<Object> findHostsWithPackage(List packages, packageName) {
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
        delegate.execute "mkdir -p ${homePath}/${PATH_ECO_SYS}/"
        def fileInputStream = resourceLoader.getResource("classpath:${PATH_CLASSPATH}/${fileName}").getInputStream()
        delegate.put from: fileInputStream, into: "${homePath}/${PATH_ECO_SYS}/${fileName}"
        def path = "${homePath}/${PATH_ECO_SYS}/${fileName}"

        log.trace("End : EcoSystemHealthcheckUtil : uploadFile")

        return path
    }
}
