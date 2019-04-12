package com.mapr.emea.ps.clustercheck.module.ecosystem.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ResourceLoader
import org.springframework.stereotype.Component

@Component
class MapRComponentHealthcheckUtil {

    static final Logger log = LoggerFactory.getLogger(MapRComponentHealthcheckUtil.class)

    static final String PATH_CLASSPATH = "/com/mapr/emea/ps/clustercheck/module/ecosystem/healthcheck"

    @Autowired
    @Qualifier("localTmpDir")
    String tmpPath

    @Autowired
    @Qualifier("maprFSTmpDir")
    String tmpMapRPath
    
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

    /**
     * Retrieve nodes where MapR Packages installed
     * @param role
     * @return
     */
    def retrievePackages(role) {

        log.trace("Start : MapRComponentHealthcheckUtil : retrievePackages")

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

        log.trace("End : MapRComponentHealthcheckUtil : retrievePackages")
        return packages
    }

    /**
     * Create the credential file for REST API call
     * @param role
     * @param credentialFileName
     * @param username
     * @param password
     * @return pathCredentialFile
     */
    def createCredentialFileREST(role, String credentialFileName, String username, String password) {
        log.trace("Start : MapRComponentHealthcheckUtil : createCredentialFileREST")

        def packages = Collections.synchronizedList([])
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                final String hostname = execute("hostname -f")
                final String content = "machine ${hostname} login ${username} password ${password}"
                executeSudo "rm -f ${tmpPath}/${credentialFileName}"
                executeSudo "echo ${content} >> ${tmpPath}/${credentialFileName}"
                executeSudo "chmod 400 ${tmpPath}/${credentialFileName}"
            }
        }

        final String pathCredentialFile = "${tmpPath}/${credentialFileName}"

        log.trace("End : MapRComponentHealthcheckUtil : createCredentialFileREST")
        return pathCredentialFile
    }

    /**
     * Create the credential file for Spyglass
     * @param credentialFileName
     * @param username
     * @param password
     * @return
     */
    def createCredentialFileSpyglass(String credentialFileName, String username, String password, delegate){
        log.trace("Start : MapRComponentHealthcheckUtil : createCredentialFileSpyglass")

        final String hostname = delegate.executeSudo "hostname -f"
        final String content = "machine ${hostname} login ${username} password ${password}"
        delegate.executeSudo "rm -f ${tmpPath}/${credentialFileName}"
        delegate.executeSudo "echo ${content} >> ${tmpPath}/${credentialFileName}"
        delegate.executeSudo "chmod 400 ${tmpPath}/${credentialFileName}"
        final String pathCredentialFile = "${tmpPath}/${credentialFileName}"

        log.trace("End : MapRComponentHealthcheckUtil : createCredentialFileSpyglass")
        return pathCredentialFile
    }

    /**
     * Delete a local file with path
     * @param role
     * @param filePath
     * @return
     */
    def deleteLocalFile(role, String filePath) {
        log.trace("Start : MapRComponentHealthcheckUtil : deleteLocalFile")

        def packages = Collections.synchronizedList([])
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                executeSudo "rm -f ${filePath}"
            }
        }

        log.trace("End : MapRComponentHealthcheckUtil : deleteLocalFile")
    }

    /**
     * Find hosts with package installed //TODO find exact package installed, instead of using "contains" (in case of drill...etc.)
     * @param packages
     * @param packageName
     * @return
     */
    static List<Object> findHostsWithPackage(List<Object> packages, String packageName) {
        log.trace("Start : MapRComponentHealthcheckUtil : findHostsWithPackage")

        def hostsFound = packages.findAll { it['mapr.packages'].find { it.contains(packageName) } != null }.collect { it['host'] }

        log.trace("End : MapRComponentHealthcheckUtil : findHostsWithPackage")

        return hostsFound
    }

    /**
     * Execute commands remotely, remote hosts are detected automatically by checking packageName
     * @param packages
     * @param packageName
     * @param closure
     * @return
     */
    def executeSsh(List<Object> packages, String packageName, Closure closure) {
        log.trace("Start : MapRComponentHealthcheckUtil : executeSsh")

        def appHosts = findHostsWithPackage(packages, packageName)

        log.debug("Found ${packageName} installed on nodes : ${appHosts}")

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

        log.trace("End : MapRComponentHealthcheckUtil : executeSsh")
        result
    }

    /**
     * Upload local file to remote host
     * @param fileName
     * @param delegate
     * @return
     */
    def uploadFileToRemoteHost(String subDir, String fileName, delegate) {
        log.trace("Start : MapRComponentHealthcheckUtil : uploadFileToRemoteHost")

        def fileInputStream = resourceLoader.getResource("classpath:${PATH_CLASSPATH}/${fileName}").getInputStream()

        String path = ""

        if(subDir){
            delegate.execute "mkdir -p ${tmpPath}/${subDir}"
            delegate.put from: fileInputStream, into: "${tmpPath}/${subDir}/${fileName}"
            path = "${tmpPath}/${subDir}/${fileName}"
        } else {
            delegate.put from: fileInputStream, into: "${tmpPath}/${fileName}"
            path = "${tmpPath}/${fileName}"
        }

        log.trace("End : MapRComponentHealthcheckUtil : uploadFileToRemoteHost")

        return path
    }

    /**
     * Upload remote file to MapR-FS
     * @param ticketfile
     * @param fileName
     * @param maprfspath
     * @param delegate
     * @return
     */
    def uploadRemoteFileToMaprfs(String subDir, String ticketfile, String filePath, delegate){
        log.trace("Start : MapRComponentHealthcheckUtil : uploadRemoteFileToMaprfs")

        String maprfspath = ""

        if(subDir){
            maprfspath = "${tmpMapRPath}/${subDir}"
            delegate.executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -mkdir -p ${maprfspath}"

        } else {
            maprfspath = "${tmpMapRPath}"
        }

        delegate.executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -put -f ${filePath} ${maprfspath}"

        log.trace("End : MapRComponentHealthcheckUtil : uploadRemoteFileToMaprfs")

        return maprfspath
    }

    /**
     * Remove MapR-FS file/directory if exists
     * @param ticketfile
     * @param fileName
     * @param delegate
     * @return
     */
    def removeMaprfsFileIfExist(String ticketfile, String fileName, delegate){
        log.trace("Start : MapRComponentHealthcheckUtil : removeMaprfsFileIfExist")

        log.info("Testing existence of MapR-FS directory: ${fileName} ... Error with status 1 when it doesn't exist.")

        def result = delegate.executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -ls ${fileName}"

        if(result.contains("No such file or directory")){
            log.debug("MapR-FS file/directory : ${fileName} doesn't exist.")
        } else {
            log.debug("${fileName} exists, will be removed.")
            delegate.executeSudo "MAPR_TICKETFILE_LOCATION=${ticketfile} hadoop fs -rm -r ${fileName}"
        }

        log.trace("End : MapRComponentHealthcheckUtil : removeMaprfsFileIfExist")
    }

}
