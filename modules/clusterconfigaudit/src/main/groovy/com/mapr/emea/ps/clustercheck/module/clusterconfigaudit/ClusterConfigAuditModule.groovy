package com.mapr.emea.ps.clustercheck.module.clusterconfigaudit

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
// TODO recommendation consider spark shuffle and sort
// TODO list 5 largest containers
// TODO show container 0 information
@ClusterCheckModule(name = "cluster-config-audit", version = "1.0")
class ClusterConfigAuditModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(ClusterConfigAuditModule.class);

    @Autowired
    @Qualifier("ssh")
    def ssh
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Override
    Map<String, ?> yamlModuleProperties() {
        return [:]
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        return []
    }

    @Override
    ClusterCheckResult execute() {
        def clusterconfigaudit = globalYamlConfig.modules['cluster-config-audit'] as Map<String, ?>
        def role = clusterconfigaudit.getOrDefault("role", "all")
        def result = Collections.synchronizedList([])
        log.info(">>>>> Running cluster-config-audit")
        ssh.runInOrder {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
                def exec = { arg -> executeSudo(arg) }
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")

                node['host'] = remote.host
                node['mapr.home.ownership'] = execute('stat --printf="%U:%G %A %n\\n" $(readlink -f /opt/mapr) || true')
                if (distribution.toLowerCase().contains("ubuntu")) {
                    node['mapr.packages'] = executeSudo('apt list --installed | grep mapr').tokenize('\n')
                } else {
                    node['mapr.packages'] = executeSudo('rpm -qa | grep mapr').tokenize('\n')
                }
                node['mapr.cldb.key.md5sum'] = executeSudo('md5sum /opt/mapr/conf/cldb.key || true').trim()
                node['mapr.clusterid'] = executeSudo('cat /opt/mapr/conf/clusterid || true').trim()
                node['mapr.ssl_truststore'] = collectMaprSslTruststore(exec)
                node['mapr.ssl_keystore'] = collectMaprSslKeystore(exec)
                node['mapr.clusters.conf'] = collectMaprClustersConf(exec)
                node['mapr.storage_pools'] = collectStoragePools(exec)
                node['mapr.env'] = collectMaprEnvSh(exec)
                node['mfs.conf'] = collectMfsConfProperties(exec)
                node['warden.conf'] = collectWardenConfProperties(exec)
                node['cldb.conf'] = collectCldbConfProperties(exec)
                node['zoo.cfg'] = collectZooCfgProperties(exec)
                node['yarn.site'] = collectYarnSiteProperties(exec)
                node['core.site'] = collectCoreSiteProperties(exec)
                node['hive.site'] = collectHiveSiteProperties(exec)
                node['hive.env'] = collectHiveEnv(exec)
                node['spark.defaults.conf'] = collectSparkDefaultsConfProperties(exec)
                node['spark.env'] = collectSparkEnv(exec)
                node['hbase.site'] = collectHbaseSiteProperties(exec)
                node['hbase.env'] = collectHbaseEnv(exec)
                node['httpfs.site'] = collectHttpfsSiteProperties(exec)
                node['httpfs.env'] = collectHttpfsEnv(exec)
                result.add(node)
            }
        }
        def groupedResult = groupSameValuesWithHosts(result)
        List recommendations = calculateRecommendations(groupedResult)
        def textReport = buildTextReport(groupedResult)
        log.info(">>>>> ... cluster-config-audit finished")
        return new ClusterCheckResult(reportJson: groupedResult, reportText: textReport, recommendations: recommendations)
    }

    def List calculateRecommendations(def groupedResult) {
        def recommendations = []
        recommendations += ifBuildGlobalMessage({
            groupedResult['mapr.home.ownership'].size() != 1
        }, "MapR Home should have same ownership on all nodes.")
        recommendations += ifBuildGlobalMessage({
            groupedResult['mapr.cldb.key.md5sum'].size() != 1
        }, "CLDB key must be the same on all nodes.")
        recommendations += ifBuildGlobalMessage({
            groupedResult['mapr.clusterid'].size() != 1
        }, "Cluster ID must be the same on all nodes.")
        recommendations += ifBuildGlobalMessage({
            groupedResult['mapr.ssl_truststore'].size() != 1
        }, "ssl_truststore should have same content on all nodes. Use a root CA to sign certificates.")
        recommendations += ifBuildGlobalMessage({
            groupedResult['mapr.clusters.conf'].size() != 1
        }, "mapr-clusters.conf must be same on all nodes.")
        def countCldbs = groupedResult['mapr.packages'].collect { c -> c['value'].count { f -> f.contains("mapr-cldb") } > 0 ? c['hosts'].size() : 0 }.sum()
        recommendations += ifBuildGlobalMessage({ countCldbs < 3 }, "There should be at least 3 CLDBs.")
        recommendations += ifBuildMessage(groupedResult, "mapr.storage_pools", { sps ->
            sps.collect {
                it.disks.size()
            }.toSet().size() > 1
        }, "The storage pools should have the same number of disks.")
        recommendations += ifBuildMessage(groupedResult, "mapr.storage_pools", { sps ->
            sps.count {
                it.state == "Offline"
            } > 0
        }, "The node has offlined storage pools!")
        recommendations += ifBuildMessage(groupedResult, "yarn.site", {
            !it.containsKey("yarn.nodemanager.local-dirs")
        }, "Consider setting the YARN NodeManager local dir to MapR-FS: https://maprdocs.mapr.com/home/Spark/ConfigureSparkwithNMLocalDirMapR-FS.html")
        recommendations
    }

    def buildTextReport(result) {
        def text = ""
        text += buildTextReportElement(result, "mapr.home.ownership", "Ownership of /opt/mapr")
        text += buildTextReportElement(result, "mapr.cldb.key.md5sum", "/opt/mapr/conf/cldb.key md5sum")
        text += buildTextReportElement(result, "mapr.clusterid", "MapR Cluster ID")
        text += buildTextReportElement(result, "mapr.clusters.conf", "MapR /opt/mapr/conf/mapr-clusters.conf")
        text += buildTextSslReportElement(result, "mapr.ssl_truststore", "MapR /opt/mapr/conf/ssl_truststore")
        text += buildTextSslReportElement(result, "mapr.ssl_keystore", "MapR /opt/mapr/conf/ssl_keystore")
        text += buildTextStoragePoolReportElement(result, "mapr.storage_pools", "MapR Storage Pools")
        text += buildTextReportElement(result, "mapr.packages", "Installed MapR packages")
        text += buildTextReportElement(result, "mfs.conf", "MapR /opt/mapr/conf/mfs.conf")
        text += buildTextReportElement(result, "cldb.conf", "MapR /opt/mapr/conf/cldb.conf")
        text += buildTextReportElement(result, "zoo.cfg", "Zookeeper configuration zoo.cfg")
        text += buildTextReportElement(result, "warden.conf", "MapR /opt/mapr/conf/warden.conf")
        text += buildTextReportElement(result, "yarn.site", "Hadoop yarn-site.xml")
        text += buildTextReportElement(result, "core.site", "Hadoop core-site.xml")
        text += buildTextReportElement(result, "hive.site", "Hive hive-site.xml")
        text += buildTextReportElement(result, "spark.defaults.conf", "Spark spark-defaults.conf")
        text += buildTextReportElement(result, "hbase.site", "MapR-DB hbase-site.xml")
        text += buildTextReportElement(result, "httpfs.site", "HttpFS httpfs-site.xml")

        // env keep it or to noisy?
        text += buildTextReportElement(result, "mapr.env", "MapR /opt/mapr/conf/env.sh")
        text += buildTextReportElement(result, "hive.env", "Hive hive-env.sh")
        text += buildTextReportElement(result, "spark.env", "Spark spark-env.sh")
        text += buildTextReportElement(result, "hbase.env", "MapR-DB hbase-env.sh")
        text += buildTextReportElement(result, "httpfs.env", "HttpFS httpfs-env.sh")
        text
    }

    def buildTextStoragePoolReportElement(result, key, description) {
        def text = "============== ${description} (${key}) ============== \n"
        for (def vals in result[key]) {
            def sps = vals['value']
            text += "------- Hosts: ${vals['hosts']} -------\n"
            sps.each { sp ->
                text += ">>> STORAGE POOL ${sp['name']} <<<\n"
                text += "Name  = ${sp['name']}\n"
                text += "Path  = ${sp['path']}\n"
                text += "State = ${sp['state']}\n"
                text += "Size  = ${sp['sizeInMB']} MB\n"
                text += "Free  = ${sp['freeInMB']} MB\n"
                text += "-----\n"
                def maxdisklen = sp.disks.collect { it.disk.size() }.max()
                text += "Disk".padRight(maxdisklen + 1, " ") + '\tSize\n'
                sp.disks.each { disk ->
                    text += disk.disk.padRight(maxdisklen + 1, " ") + '\t' + disk.sizeInMB + " MB\n"
                }
                text += ">>> END STORAGE POOL <<<\n"
            }
            text += '\n'
        }
        text += "\n"
        text
    }

    def buildTextSslReportElement(result, key, description) {
        def text = "============== ${description} (${key}) ============== \n"
        for (def vals in result[key]) {
            def value = vals['value']
            text += "------- Hosts: ${vals['hosts']} -------\n"
            if (value.containsKey("error")) {
                text += "${value['error']}\n"
            } else {
                text += "File: ${value['filename']}\n"
                text += "MD5sum: ${value['md5sum']}\n"
                // mapping here
                value['entries'].each { entry ->
                    text += ">>> ENTRY ${entry['aliasName']} <<<\n"
                    text += "Alias name             = ${entry['aliasName']}\n"
                    text += "Creation Date          = ${entry['creationDate']}\n"
                    text += "Entry Type             = ${entry['entryType']}\n"
                    text += "Owner                  = ${entry['owner']}\n"
                    text += "Issuer                 = ${entry['issuer']}\n"
                    text += "Serial Number          = ${entry['serialNumber']}\n"
                    text += "Valid From             = ${entry['validFrom']}\n"
                    text += "Valid Until            = ${entry['validUntil']}\n"
                    text += "Fingerprint MD5        = ${entry['fingerprintMd5']}\n"
                    text += "Fingerprint SHA1       = ${entry['fingerprintSha1']}\n"
                    text += "Fingerprint SHA256     = ${entry['fingerprintSha256']}\n"
                    text += "Signature Algorithm    = ${entry['signatureAlgorithm']}\n"
                    text += "Subject Public Key Alg = ${entry['subjectPublicKeyAlgorithm']}\n"
                    text += "Version                = ${entry['version']}\n"
                    text += ">>> END ENTRY <<<\n"
                }
                text += '\n'
            }
        }
        text += "\n"
        text
    }

    def buildTextReportElement(result, key, description) {
        def text = "============== ${description} (${key}) ============== \n"
        for (def vals in result[key]) {
            text += "------- Hosts: ${vals['hosts']} -------\n"
            if (vals['value'] instanceof Collection) {
                text += vals['value'].join('\n') + '\n'
            } else if (vals['value'] instanceof Map) {
                for (def m in vals['value']) {
                    text += m.key + " = " + m.value + "\n"
                }
            } else {
                text += "${vals['value']}\n"
            }
            text += '\n'
        }
        text += "\n"
        text
    }

    def ifBuildGlobalMessage(Closure<Boolean> condition, String message) {
        if (condition()) {
            return [message]
        }
        return []
    }

    def ifBuildMessage(def result, String key, Closure<Boolean> condition, String message) {
        def hosts = result[key].findAll { condition(it['value']) }['hosts'].flatten()
        return hosts.collect { "${it}: ${message}" }
    }

    private static def groupSameValuesWithHosts(def nodes) {
        def result = [:]
        for (def node : nodes) {
            def host = node['host']
            for (def e in node) {
                if (e.key != "host") {
                    if (!result.containsKey(e.key)) {
                        result[e.key] = [['hosts': [host] as Set, value: e.value]]
                    } else {
                        def values = result[e.key]
                        def found = false
                        for (def value : values) {
                            if (value['value'] == node[e.key]) {
                                found = true
                                value['hosts'] << host
                            }
                        }
                        if (!found) {
                            result[e.key] << ['hosts': [host] as Set, value: e.value]
                        }
                    }
                }
            }
        }
        result
    }

    def collectStoragePools(execute) {
        def spList = execute("/opt/mapr/server/mrconfig sp list || true")
        def lines = spList.tokenize('\n')
        def spLines = lines.findAll { it.startsWith("SP") }.collect { it.substring(it.indexOf(':') + 1).trim() }
        def storagePools = spLines.collect { line ->
            def items = line.tokenize(',').collect { it.trim() }
            def values = [:]
            items.each { itemStr ->
                def item = itemStr.tokenize(' ')
                if (item[0] == "name") {
                    values['name'] = item[1]
                } else if (item[0] == "Online" || item[0] == "Offline") {
                    values['state'] = item[0]
                } else if (item[0] == "size") {
                    values['sizeInMB'] = item[1] as Long
                } else if (item[0] == "free") {
                    values['freeInMB'] = item[1] as Long
                } else if (item[0] == "path") {
                    values['path'] = item[1]
                }
            }
            return values
        }
        def diskList = execute("/opt/mapr/server/mrconfig disk list || true")
        def diskLines = diskList.tokenize('\n')
        def disks = []
        def currentDisk = [:]
        diskLines.each { lineUntrimmed ->
            def line = lineUntrimmed.trim()
            if (line.startsWith("ListDisks") && !line.contains("ListDisks resp")) {
                currentDisk['disk'] = line.tokenize(' ')[1].trim()
            }
            if (line.startsWith("size")) {
                currentDisk['sizeInMB'] = line.findAll(/\d+/)[0] as int
            }
            if (line.startsWith("SP")) {
                // add line
                def spLine = line.substring(line.indexOf(':') + 1).trim()
                def items = spLine.tokenize(',').collect { it.trim() }
                items.each { itemStr ->
                    def item = itemStr.tokenize(' ')
                    if (item[0] == "name") {
                        currentDisk['storagePoolName'] = item[1]
                    }
                }
                disks.add(currentDisk)
                currentDisk = [:]
            }
        }

        return storagePools.collect { sp ->
            sp['disks'] = disks.findAll { it['storagePoolName'] == sp['name'] }
            return sp
        }
    }


    def collectMaprSslKeystore(execute) {
        return collectMaprSslStore(execute, "/opt/mapr/conf/ssl_keystore")
    }

    def collectMaprSslTruststore(execute) {
        return collectMaprSslStore(execute, "/opt/mapr/conf/ssl_truststore")
    }

    def collectMaprSslStore(execute, filename) {
        def md5sum = execute("md5sum ${filename} || true").trim()
        if (!md5sum.contains("No such file")) {
            def result = [:]
            result['md5sum'] = md5sum.tokenize(' ')[0].trim()
            result['filename'] = filename
            def sslTruststore = execute("keytool -list -v -keystore ${filename} -storepass mapr123")
            def current = [:]
            def entries = []
            if (sslTruststore.contains("Your keystore contains")) {
                def lines = sslTruststore.tokenize('\n')
                lines.each { line ->
                    if (line.startsWith("Alias name:")) {
                        current['aliasName'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Creation date:")) {
                        current['creationDate'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Entry type:")) {
                        current['entryType'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Owner:")) {
                        current['owner'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Issuer:")) {
                        current['issuer'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Serial number:")) {
                        current['serialNumber'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Valid from:")) {
                        def dateBlock = line.substring(line.indexOf(':') + 1).trim()
                        def indexUntil = dateBlock.indexOf(" until: ")
                        current['validFrom'] = dateBlock.substring(0, indexUntil).trim()
                        current['validUntil'] = dateBlock.substring(indexUntil + 7).trim()
                    } else if (line.startsWith("Signature algorithm name:")) {
                        current['signatureAlgorithm'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Subject Public Key Algorithm:")) {
                        current['subjectPublicKeyAlgorithm'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.contains("MD5:")) {
                        current['fingerprintMd5'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.contains("SHA1:")) {
                        current['fingerprintSha1'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.contains("SHA256:")) {
                        current['fingerprintSha256'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if (line.startsWith("Version:")) {
                        current['version'] = line.substring(line.indexOf(':') + 1).trim()
                        entries.add(current)
                        current = [:]
                    }
                }

            }
            result['entries'] = entries
            return result
        }
        return [error: "File not found"]
    }

    def collectTextFile(file, execute) {
        def envSh = execute("cat ${file} || true").trim()
        if (!envSh.contains("No such file")) {
            return envSh.tokenize('\n')
        }
        return ["File not found"]
    }

    def collectMaprEnvSh(execute) {
        return collectTextFile("/opt/mapr/conf/env.sh", execute)
    }

    def collectMaprClustersConf(execute) {
        return collectTextFile("/opt/mapr/conf/mapr-clusters.conf", execute)
    }

    def collectProperties(file, execute) {
        try {
            def conf = execute("cat ${file} || true").trim()
            if (!conf.contains("No such file")) {
                def prop = new Properties();
                prop.load(new ByteArrayInputStream(conf.getBytes()))
                return prop.toSpreadMap()
            }
        } catch (ex) {
            return ["error": ex.getMessage()]
        }
        return [error: "File not found"]
    }

    def collectWardenConfProperties(execute) {
        return collectProperties("/opt/mapr/conf/warden.conf", execute)
    }

    def collectSparkDefaultsConfProperties(execute) {
        return collectProperties("/opt/mapr/spark/spark-*/conf/spark-defaults.conf", execute)
    }

    def collectSparkEnv(execute) {
        return collectTextFile("/opt/mapr/spark/spark-*/conf/spark-env.sh", execute)
    }

    def collectMfsConfProperties(execute) {
        return collectProperties("/opt/mapr/conf/mfs.conf", execute)
    }

    def collectCldbConfProperties(execute) {
        return collectProperties("/opt/mapr/conf/cldb.conf", execute)
    }

    def collectZooCfgProperties(execute) {
        def zookeeperVersion = execute("cat /opt/mapr/zookeeper/zookeeperversion || true").trim()
        if (!zookeeperVersion.contains("No such file")) {
            return collectProperties("/opt/mapr/zookeeper/zookeeper-${zookeeperVersion}/conf/zoo.cfg", execute)
        }
        return [info: "Zookeeper not installed on node"]
    }

    def collectXmlSite(file, execute) {
        try {
            def site = execute("cat ${file} || true").trim()
            if (!site.contains("No such file")) {
                def siteParsed = new XmlSlurper().parseText(site)
                return siteParsed.property.collectEntries {
                    [it.name.toString(), it.value.toString()]
                }
            }
        } catch (ex) {
            return ["error": ex.getMessage()]
        }
        return [error: "File not found"]
    }

    def collectHttpfsSiteProperties(execute) {
        //    def httpfsversion = execute("cat /opt/mapr/httpfs/httpfsversion || true").trim()
        //    if (!httpfsversion.contains("No such file")) {
        return collectXmlSite("/opt/mapr/httpfs/httpfs-*/etc/hadoop/httpfs-site.xml", execute)
        //    }
        //    return [info: "HttpFS is not installed"]
    }

    def collectHttpfsEnv(execute) {
        //    def hbaseEnvVersion = execute("cat /opt/mapr/httpfs/httpfsversion || true").trim()
        //    if (!hbaseEnvVersion.contains("No such file")) {
        return collectTextFile("/opt/mapr/httpfs/httpfs-*/etc/hadoop/httpfs-env.sh", execute)
        //    }
        //    return ["HttpFS is not installed"]
    }

    def collectYarnSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion || true").trim()
        if (!hadoopVersion.contains("No such file")) {
            return collectXmlSite("/opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/yarn-site.xml", execute)
        }
        return [info: "Hadoop is not installed"]
    }

    def collectHbaseSiteProperties(execute) {
        def hbaseVersion = execute("cat /opt/mapr/hbase/hbaseversion || true").trim()
        if (!hbaseVersion.contains("No such file")) {
            return collectXmlSite("/opt/mapr/hbase/hbase-${hbaseVersion}/conf/hbase-site.xml", execute)
        }
        return [info: "HBase client is not installed"]
    }

    def collectHbaseEnv(execute) {
        def hbaseVersion = execute("cat /opt/mapr/hbase/hbaseversion || true").trim()
        if (!hbaseVersion.contains("No such file")) {
            return collectTextFile("/opt/mapr/hbase/hbase-${hbaseVersion}/conf/hbase-env.sh", execute)
        }
        return ["HBase client is not installed"]
    }

    def collectCoreSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion || true").trim()
        if (!hadoopVersion.contains("No such file")) {
            return collectXmlSite("/opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/core-site.xml", execute)
        }
        return [info: "Hadoop is not installed"]
    }

    def collectHiveSiteProperties(execute) {
        //    def hiveVersion = execute("cat /opt/mapr/hive/hiveversion || true").trim()
        //    if (!hiveVersion.contains("No such file")) {
        return collectXmlSite("/opt/mapr/hive/hive-*/conf/hive-site.xml", execute)
        //    }
        //    return [info: "Hive is not installed"]

    }

    def collectHiveEnv(execute) {
        //    def hiveVersion = execute("cat /opt/mapr/hive/hiveversion || true").trim()
        //    if (!hiveVersion.contains("No such file")) {
        return collectTextFile("/opt/mapr/hive/hive-*/conf/hive-env.sh", execute)
        //    }
        //    return ["Hive is not installed"]
    }
}
