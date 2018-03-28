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
// TODO implement TEXT report
// TODO implement diffs and give recommendations

// TODO recommendation nm-local-dir
// TODO recommentation: at least 3 CLDB
// TODO dump env files to separate dir?

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
                node['host'] = remote.host
                node['maprHomeOwnership'] = execute('stat --printf="%U:%G %A %n\\n" $(readlink -f /opt/mapr)')
                node['maprPackages'] = execute('rpm -qa | grep mapr').tokenize('\n')
                node['storagePools'] = collectStoragePools(exec)
                node['maprSslTruststore'] = collectMaprSslTruststore(exec)
                node['maprSslKeystore'] = collectMaprSslKeystore(exec)
                node['maprClustersConf'] = collectMaprClustersConf(exec)
                node['mfsConf'] = collectMfsConfProperties(exec)
                node['wardenConf'] = collectWardenConfProperties(exec)
                node['cldbConf'] = collectCldbConfProperties(exec)
                node['zooCfg'] = collectZooCfgProperties(exec)
                node['yarnSite'] = collectYarnSiteProperties(exec)
                node['coreSite'] = collectCoreSiteProperties(exec)
                node['hiveSite'] = collectHiveSiteProperties(exec)
                node['hiveEnv'] = collectHiveEnv(exec)
                node['maprEnvSh'] = collectMaprEnvSh(exec)
                node['sparkDefaultsConf'] = collectSparkDefaultsConfProperties(exec)
                node['sparkEnv'] = collectSparkEnv(exec)
                node['hbaseSite'] = collectHbaseSiteProperties(exec)
                node['hbaseEnv'] = collectHbaseEnv(exec)
                node['httpfsSite'] = collectHttpfsSiteProperties(exec)
                node['httpfsEnv'] = collectHttpfsEnv(exec)
                result.add(node)
            }
        }
        def groupedResult = groupSameValuesWithHosts(result)
        log.info(">>>>> ... cluster-config-audit finished")
        return new ClusterCheckResult(reportJson: groupedResult, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
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
        def spList = execute("/opt/mapr/server/mrconfig sp list")
        def lines = spList.tokenize('\n')
        def spLines = lines.findAll { it.startsWith("SP") }.collect{ it.substring(it.indexOf(':') + 1).trim() }
        def storagePools = spLines.collect { line ->
            def items = line.tokenize(',').collect { it.trim() }
            def values = [:]
            items.each { itemStr ->
                def item = itemStr.tokenize(' ')
                if(item[0] == "name") {
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
        def diskList = execute("/opt/mapr/server/mrconfig disk list")
        def diskLines = diskList.tokenize('\n')
        def disks = []
        def currentDisk = [:]
        diskLines.each { lineUntrimmed ->
            def line = lineUntrimmed.trim()
            if(line.startsWith("ListDisks") && !line.contains("ListDisks resp")) {
                currentDisk['disk'] = line.tokenize(' ')[1].trim()
            }
            if(line.startsWith("size")) {
                currentDisk['sizeInMB'] = line.findAll( /\d+/ )[0] as int
            }
            if(line.startsWith("SP")) {
                // add line
                def spLine = line.substring(line.indexOf(':') + 1).trim()
                def items = spLine.tokenize(',').collect { it.trim() }
                items.each { itemStr ->
                    def item = itemStr.tokenize(' ')
                    if(item[0] == "name") {
                        currentDisk['storagePoolName'] = item[1]
                    }
                }
                disks.add(currentDisk)
                currentDisk = [:]
            }
        }

        return storagePools.collect { sp ->
            sp['disks'] = disks.findAll { it['storagePoolName'] == sp['name']}
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
        def md5sum = execute("md5sum ${filename}").trim()
        if (!md5sum.contains("No such file")) {
            def result = [:]
            result['md5sum'] = md5sum.tokenize(' ')[0].trim()
            result['filename'] = filename
            def sslTruststore = execute("keytool -list -v -keystore ${filename} -storepass mapr123")
            def current = [:]
            def entries = []
            if(sslTruststore.contains("Your keystore contains")) {
                def lines = sslTruststore.tokenize('\n')
                lines.each { line ->
                    if(line.startsWith("Alias name:")) {
                        current['aliasName'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Creation date:")) {
                        current['creationDate'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Entry type:")) {
                        current['entryType'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Owner:")) {
                        current['owner'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Issuer:")) {
                        current['issuer'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Serial number:")) {
                        current['serialNumber'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Valid from:")) {
                         def dateBlock = line.substring(line.indexOf(':') + 1).trim()
                         def indexUntil = dateBlock.indexOf(" until: ")
                        current['validFrom'] = dateBlock.substring(0, indexUntil).trim()
                        current['validUntil'] = dateBlock.substring(indexUntil + 7).trim()
                    } else if(line.startsWith("Signature algorithm name:")) {
                        current['signatureAlgorithm'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.startsWith("Subject Public Key Algorithm:")) {
                        current['subjectPublicKeyAlgorithm'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.contains("MD5:")) {
                        current['fingerprintMd5'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.contains("SHA1:")) {
                        current['fingerprintSha1'] = line.substring(line.indexOf(':') + 1).trim()
                    } else if(line.contains("SHA256:")) {
                        current['fingerprintSha256'] = line.substring(line.indexOf(':') + 1).trim()
                    }
                    else if(line.startsWith("Version:")) {
                        current['version'] = line.substring(line.indexOf(':') + 1).trim()
                        entries.add(current)
                        current = [:]
                    }
                }

            }
            result['entries'] = entries
            return result
        }
        return ["File not found"]
    }

    def collectMaprEnvSh(execute) {
        def envSh = execute("cat /opt/mapr/conf/env.sh").trim()
        if (!envSh.contains("No such file")) {
            return envSh.tokenize('\n')
        }
        return ["File not found"]
    }

    def collectMaprClustersConf(execute) {
        def maprClustersConf = execute("cat /opt/mapr/conf/mapr-clusters.conf").trim()
        if (!maprClustersConf.contains("No such file")) {
            return maprClustersConf.tokenize('\n')
        }
        return ["File not found"]
    }

    def collectWardenConfProperties(execute) {
        def wardenConf = execute("cat /opt/mapr/conf/warden.conf").trim()
        if (!wardenConf.contains("No such file")) {
            def prop = new Properties();
            prop.load(new ByteArrayInputStream(wardenConf.getBytes()))
            return prop.toSpreadMap()
        }
        return [:]
    }

    def collectSparkDefaultsConfProperties(execute) {
        def sparkConf = execute("cat /opt/mapr/spark/spark-*/conf/spark-defaults.conf").trim()
        if (!sparkConf.contains("No such file")) {
            def prop = new Properties();
            prop.load(new ByteArrayInputStream(sparkConf.getBytes()))
            return prop.toSpreadMap()
        }
        return [:]
    }

    def collectSparkEnv(execute) {
        def sparkEnv = execute("cat /opt/mapr/spark/spark-*/conf/spark-env.sh").trim()
        if (!sparkEnv.contains("No such file")) {
            return sparkEnv.tokenize('\n')
        }
        return ""
    }

    def collectMfsConfProperties(execute) {
        def mfsConf = execute("cat /opt/mapr/conf/mfs.conf").trim()
        if (!mfsConf.contains("No such file")) {
            def prop = new Properties();
            prop.load(new ByteArrayInputStream(mfsConf.getBytes()))
            return prop.toSpreadMap()
        }
        return [:]
    }

    def collectCldbConfProperties(execute) {
        def cldbConf = execute("cat /opt/mapr/conf/cldb.conf").trim()
        if (!cldbConf.contains("No such file")) {
            def prop = new Properties();
            prop.load(new ByteArrayInputStream(cldbConf.getBytes()))
            return prop.toSpreadMap()
        }
        return [:]
    }

    def collectZooCfgProperties(execute) {
        def zookeeperVersion = execute("cat /opt/mapr/zookeeper/zookeeperversion").trim()
        if (!zookeeperVersion.contains("No such file")) {
            def zooCfg = execute("cat /opt/mapr/zookeeper/zookeeper-${zookeeperVersion}/conf/zoo.cfg").trim()
            def prop = new Properties();
            prop.load(new ByteArrayInputStream(zooCfg.getBytes()))
            return prop.toSpreadMap()
        }
        return [:]
    }

    def collectHttpfsSiteProperties(execute) {
        def httpfsversion = execute("cat /opt/mapr/httpfs/httpfsversion").trim()
        if (!httpfsversion.contains("No such file")) {
            def site = execute("cat /opt/mapr/httpfs/httpfs-${ httpfsversion }/etc/hadoop/httpfs-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHttpfsEnv(execute) {
        def hbaseEnvVersion = execute("cat /opt/mapr/httpfs/httpfsversion").trim()
        if (!hbaseEnvVersion.contains("No such file")) {
            return execute("cat /opt/mapr/httpfs/httpfs-${hbaseEnvVersion}/etc/hadoop/httpfs-env.sh").trim().tokenize('\n')
        }
        return ""
    }

    def collectYarnSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/yarn-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHbaseSiteProperties(execute) {
        def hbaseVersion = execute("cat /opt/mapr/hbase/hbaseversion").trim()
        if (!hbaseVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hbase/hbase-${hbaseVersion}/conf/hbase-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHbaseEnv(execute) {
        def hbaseEnvVersion = execute("cat /opt/mapr/hbase/hbaseversion").trim()
        if (!hbaseEnvVersion.contains("No such file")) {
            return execute("cat /opt/mapr/hbase/hbase-${hbaseEnvVersion}/conf/hbase-env.sh").trim().tokenize('\n')
        }
        return ""
    }

    def collectCoreSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hadoop/hadoopversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hadoop/hadoop-${hadoopVersion}/etc/hadoop/core-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHiveSiteProperties(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hive/hiveversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            def site = execute("cat /opt/mapr/hive/hive-${hadoopVersion}/conf/hive-site.xml").trim()
            def siteParsed = new XmlSlurper().parseText(site)
            return siteParsed.property.collectEntries {
                [it.name.toString(), it.value.toString()]
            }
        }
        return [:]
    }

    def collectHiveEnv(execute) {
        def hadoopVersion = execute("cat /opt/mapr/hive/hiveversion").trim()
        if (!hadoopVersion.contains("No such file")) {
            return execute("cat /opt/mapr/hive/hive-${hadoopVersion}/conf/hive-env.sh").trim().tokenize('\n')
        }
        return ""
    }


}
