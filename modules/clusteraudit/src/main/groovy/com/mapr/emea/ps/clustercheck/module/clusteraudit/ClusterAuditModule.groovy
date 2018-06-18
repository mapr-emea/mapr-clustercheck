package com.mapr.emea.ps.clustercheck.module.clusteraudit

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

@ClusterCheckModule(name = "cluster-audit", version = "1.0")
class ClusterAuditModule implements ExecuteModule {
    static final Logger log = LoggerFactory.getLogger(ClusterAuditModule.class);

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
        def clusteraudit = globalYamlConfig.modules['cluster-audit'] as Map<String, ?>
        def role = clusteraudit.getOrDefault("role", "all")
        def warnings = Collections.synchronizedList([])
        ssh.run {
            settings {
                timeoutSec = 10
                pty = true
            }
            session(ssh.remotes.role(role)) {
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                if (distribution.toLowerCase().contains("ubuntu")) {
                    def result = execute("dpkg -l pciutils dmidecode net-tools ethtool bind9utils > /dev/null || true").trim()
                    if (result) {
                        warnings << ("Please install following tools: " + result)
                    }

                } else {
                    def result = execute("rpm -q pciutils dmidecode net-tools ethtool bind-utils | grep 'is not installed' || true").trim()
                    if (result) {
                        warnings << ("Please install following tools: \n " + result)
                    }
                }
            }
        }
        return warnings

    }

    @Override
    ClusterCheckResult execute() {
        def clusteraudit = globalYamlConfig.modules['cluster-audit'] as Map<String, ?>
        def role = clusteraudit.getOrDefault("role", "all")
        def mapruser = globalYamlConfig.getOrDefault("mapr_user", "mapr")
        def nodes = Collections.synchronizedList([])
        log.info(">>>>> Running cluster-audit")
        ssh.run {
            settings {
                pty = true
                timeoutSec = 10
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
                node['host'] = remote.host
                node['hostname'] = execute 'hostname -f'
                // Sysinfo
                def dmicodeSysInfo = executeSudo('dmidecode | grep -A2 \'^System Information\'')
                node['sysinfo.manufacturer'] = getColonProperty(dmicodeSysInfo, "Manufacturer")
                node['sysinfo.product_name'] = getColonProperty(dmicodeSysInfo, "Product Name")

                // BIOS
                def bios = executeSudo('dmidecode | grep -A3 \'^BIOS I\'')
                node['bios.vendor'] = getColonProperty(bios, "Vendor")
                node['bios.version'] = getColonProperty(bios, "Version")
                node['bios.release_date'] = getColonProperty(bios, "Release Date")

                // CPU
                def cpu = execute('lscpu')
                node['cpu.model'] = getColonProperty(cpu, 'Model name')
                node['cpu.architecture'] = getColonProperty(cpu, 'Architecture')
                node['cpu.virtualization'] = getColonProperty(cpu, 'Virtualization type')
                node['cpu.bogo_mips'] = getColonProperty(cpu, 'BogoMIPS')
                node['cpu.cores'] = getColonProperty(cpu, 'CPU(s)')
                node['cpu.stepping'] = getColonProperty(cpu, 'Stepping')
                node['cpu.byte_order'] = getColonProperty(cpu, 'Byte Order')
                node['cpu.cpu_mhz'] = getColonProperty(cpu, 'CPU MHz')
                node['cpu.threads_per_core'] = getColonProperty(cpu, 'Thread(s) per core')

                // Memory
                def memory = execute('cat /proc/meminfo')
                node['memory.total'] = getColonProperty(memory, "MemTotal")
                node['memory.free'] = getColonProperty(memory, "MemFree")
                node['memory.swap_cached'] = getColonProperty(memory, "SwapCached")
                node['memory.swap_total'] = getColonProperty(memory, "SwapTotal")
                node['memory.swap_free'] = getColonProperty(memory, "SwapFree")
                node['memory.hugepage_total'] = getColonProperty(memory, "HugePages_Total")
                node['memory.hugepage_free'] = getColonProperty(memory, "HugePages_Free")
                node['memory.dimm_slots'] = executeSudo('dmidecode -t memory |grep -c \'^[[:space:]]*Locator:\'')
                node['memory.dimm_count'] = executeSudo('dmidecode -t memory | grep -c \'^[[:space:]]Size: [0-9][0-9]*\'')
                node['memory.dimm_info'] = executeSudo('dmidecode -t memory | awk \'/Memory Device$/,/^$/ {print}\'')

                // NIC / Ethernet

                def executeSudo = { arg -> executeSudo(arg) }
                def ethernetControllers = findLine(executeSudo('lspci'), 'Ethernet controller:')
                def ifconfig = executeSudo('ifconfig -a')
                def ifconfigMap = parseInterfaceToMap(ifconfig, executeSudo)
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                def systemd = execute("[ -f /etc/systemd/system.conf ] && echo true || echo false")
                node['ethernet.controller'] = getColonProperty(ethernetControllers, "Ethernet controller:")
                node << ifconfigMap
                node['storage.controller'] = executeSudo("lspci | grep -i -e ide -e raid -e storage -e lsi || true")
                node['storage.scsi_raid'] = executeSudo("dmesg | grep -i raid | grep -i -o 'scsi.*\$' | uniq || true")
                node['storage.disks'] = executeSudo("fdisk -l | grep '^Disk /.*:' |sort").tokenize('\n')
                node['storage.udev_rules'] = executeSudo("ls /etc/udev/rules.d")
                node['os.distribution'] = distribution
                node['os.kernel'] = execute("uname -srvmo | fmt")
                node['os.time'] = execute("date")
                node['os.locale'] = getColonValueFromLines(executeSudo("su - ${mapruser} -c 'locale | grep LANG' || true"), "LANG=")

                if (distribution.toLowerCase().contains("ubuntu")) {
                    node['os.umask'] = executeSudo("su -c umask")
                    node['os.services.apparmor'] = executeSudo("apparmor_status | sed 's/([0-9]*)//' || true")
                    node['os.services.selinux'] = execute("([ -d /etc/selinux -a -f /etc/selinux/config ] && grep ^SELINUX= /etc/selinux/config) || echo 'Disabled'")
                    node['os.services.firewall'] = executeSudo("service ufw status | head -10 || true").tokenize('\n')
                    node['os.services.iptables'] = executeSudo("iptables -L | head -10 || true").tokenize('\n')
                    node['os.packages.nfs'] = execute("dpkg -l '*nfs*' | grep ^i || true").tokenize('\n')
                } else {
                    node['os.umask'] = executeSudo("umask")
                    if (distribution.toLowerCase().contains("sles")) {
                        node['os.repositories'] = execute("zypper repos | grep -i mapr").tokenize('\n')
                        node['os.selinux'] = execute("rpm -q selinux-tools selinux-policy")
                        node['os.firewall'] = executeSudo("service SuSEfirewall2_init status").tokenize('\n')
                    } else {
                        node['os.repositories'] = executeSudo("yum --noplugins repolist | grep -i mapr || true").tokenize('\n')
                        node['os.selinux'] = executeSudo("getenforce")

                    }
                    node['os.packages.nfs'] = execute("rpm -qa | grep -i nfs | sort").tokenize('\n')
                    node['os.packages.required'] = execute("rpm -q dmidecode bind-utils irqbalance syslinux hdparm sdparm rpcbind nfs-utils redhat-lsb-core ntp | grep 'is not installed' || true").tokenize('\n')
                    node['os.packages.optional'] = execute("rpm -q patch nc dstat xml2 jq git tmux zsh vim nmap mysql mysql-server tuned smartmontools pciutils lsof lvm2 iftop ntop iotop atop ftop htop ntpdate tree net-tools ethtool | grep 'is not installed' || true").tokenize('\n')
                }

                if (systemd == "true") {
                    if (distribution.toLowerCase().contains("ubuntu")) {
                        node['os.services.ntpd'] = executeSudo("systemctl status ntp || true").tokenize('\n')
                    }
                    else {
                        node['os.services.ntpd'] = executeSudo("systemctl status ntpd || true").tokenize('\n')
                    }
                    node['os.services.sssd'] = executeSudo("systemctl status sssd || true").tokenize('\n')
                    node['os.services.firewall'] = executeSudo("systemctl status firewalld || true").tokenize('\n')
                    node['os.services.iptables'] = executeSudo("systemctl status iptables || true").tokenize('\n')
                    node['os.services.cpuspeed'] = executeSudo("systemctl status cpuspeed || true").tokenize('\n')

                } else {
                    if (distribution.toLowerCase().contains("ubuntu")) {
                        node['os.services.ntpd'] = executeSudo("service ntp status || true").tokenize('\n')
                    }
                    else {
                        node['os.services.ntpd'] = executeSudo("service ntpd status || true").tokenize('\n')
                    }
                    node['os.services.sssd'] = executeSudo("service sssd status || true").tokenize('\n')
                    node['os.services.iptables'] = executeSudo("service iptables status | head -10 || true").tokenize('\n')
                    node['os.services.cpuspeed'] = executeSudo("chkconfig --list cpuspeed || true").tokenize('\n')
                }

                node['os.kernel_params.vm.swappiness'] = getColonValue(executeSudo("sysctl vm.swappiness"), '=')
                node['os.kernel_params.net.ipv4.tcp_retries2'] = getColonValue(executeSudo("sysctl net.ipv4.tcp_retries2"), '=')
                node['os.kernel_params.net.ipv4.tcp_fin_timeout'] = getColonValue(executeSudo("sysctl net.ipv4.tcp_fin_timeout"), '=')
                node['os.kernel_params.vm.overcommit_memory'] = getColonValue(executeSudo("sysctl vm.overcommit_memory"), '=')

                node['os.thp'] = executeSudo("cat /sys/kernel/mm/transparent_hugepage/enabled")
                node['storage.luks'] = executeSudo("grep -v -e ^# -e ^\$ /etc/crypttab | uniq -c -f2 || true")
                node['storage.controller_max_transfer_size'] = executeSudo("files=\$(ls /sys/block/{sd,xvd,vd}*/queue/max_hw_sectors_kb 2>/dev/null); for each in \$files; do printf \"%s: %s\\n\" \$each \$(cat \$each); done |uniq -c -f1")
                node['storage.controller_configured_transfer_size'] = executeSudo("files=\$(ls /sys/block/{sd,xvd,vd}*/queue/max_sectors_kb 2>/dev/null); for each in \$files; do printf \"%s: %s\\n\" \$each \$(cat \$each); done |uniq -c -f1")
                if (systemd == "true") {
                    node['storage.mounted_fs'] = executeSudo("df -h --output=fstype,size,pcent,target -x tmpfs -x devtmpfs").tokenize('\n')
                } else {
                    node['storage.mounted_fs'] = executeSudo("df -hT | cut -c22-28,39- | grep -e '  *' | grep -v -e /dev").tokenize('\n')
                }
                node['storage.mount_permissions'] = executeSudo("mount | grep -e noexec -e nosuid | grep -v tmpfs |grep -v 'type cgroup'").tokenize('\n')
                node['storage.tmp_dir_permission'] = executeSudo("stat -c %a /tmp")
                def java_version_output = execute("java -version || true")
                node['java.openjdk'] = java_version_output.toLowerCase().contains("openjdk")
                node['java.version'] = getColonValueFromLines(java_version_output, "version").replace('"', '')
                node['java.version_output'] = java_version_output.tokenize('\n')

                if (distribution.toLowerCase().contains("sles")) {
                    node['ip'] = execute('hostname -i')
                } else {
                    node['ip'] = execute('hostname -I')
                }
                node['ulimit.mapr_processes'] = executeSudo("su - ${mapruser} -c 'ulimit -u' || true")
                node['ulimit.mapr_files'] = executeSudo("su - ${mapruser} -c 'ulimit -n' || true")
                node['ulimit.limits_conf'] = executeSudo("grep -e nproc -e nofile /etc/security/limits.conf |grep -v ':#'")
                node['ulimit.limits_d_conf'] = executeSudo("[ -d /etc/security/limits.d ] && (grep -e nproc -e nofile /etc/security/limits.d/*.conf |grep -v ':#') || true")
                node['mapr.system.user'] = executeSudo("id ${mapruser} || true")


                node['dns.lookup'] = execute("host -W 10 " + node['hostname'] + " || true")
                node['dns.reverse'] = execute("host -W 10 " + node['ip'] + " || true")

                // MapR Cluster
                def maprcli_alarm_output = executeSudo(suStr("maprcli alarm list | tail -n +2 | awk '\"'\"'{print \$(NF-1)}'\"'\"'"))
                if(maprcli_alarm_output.toLowerCase().contains("alarm")){
                    node['cluster.alarms'] = maprcli_alarm_output.tokenize('\n')
                }
   

                nodes.add(node)
            }
        }
        def result = groupSameValuesWithHosts(nodes)


        def recommendations = []
        recommendations += calculateRecommendationsForDiffValues(result)
        recommendations += ifBuildMessage(result, "os.thp", {
            it.contains("[always]")
        }, "Disable Transparent Huge Pages.")
        recommendations += ifBuildMessage(result, "ulimit.mapr_processes", {
            it != "unlimited" || (!(it =~ /^[0-9]+$/) || (it as int) <= 64000)
        }, "Set MapR system user process limit to a minimum of 64000.")
        recommendations += ifBuildMessage(result, "ulimit.mapr_files", {
            it != "unlimited" || (!(it =~ /^[0-9]+$/) || (it as int) <= 64000)
        }, "Set MapR system user files limit to a minimum of 64000.")
        recommendations += ifBuildMessage(result, "os.kernel_params.vm.swappiness", {
            it != "10"
        }, "Set kernel parameter vm.swappiness=10")
        recommendations += ifBuildMessage(result, "os.kernel_params.net.ipv4.tcp_retries2", {
            it != "5"
        }, "Set kernel parameter net.ipv4.tcp_retries2=5")
        recommendations += ifBuildMessage(result, "os.kernel_params.net.ipv4.tcp_fin_timeout", {
            it == "60"
        }, "Reduce the default kernel parameter net.ipv4.tcp_fin_timeout from 60 to 30 can help the cluster performing faster")
        recommendations += ifBuildMessage(result, "os.kernel_params.vm.overcommit_memory", {
            it != "0"
        }, "Set kernel parameter os.kernel_params.vm.overcommit_memory=0")
        recommendations += ifBuildMessage(result, "os.packages.required", {
            it
        }, "Please install all OS required packages.")
        recommendations += ifBuildMessage(result, "os.umask", { it != "0022" }, "Default umask must be '0022'")
        recommendations += ifBuildMessage(result, "os.selinux", { it != "Disabled" }, "SE Linux should be disabled.")
        recommendations += ifBuildMessage(result, "os.locale", {
            it != "en_US.UTF-8"
        }, "OS locale should be 'en_US.UTF-8'.")
        recommendations += ifBuildMessage(result, "dns.lookup", {
            !it.contains("has address")
        }, "DNS lookup for host does not work.")
        recommendations += ifBuildMessage(result, "dns.reverse", {
            !it.contains("domain name pointer")
        }, "Reverse DNS lookup for host does not work.")
        recommendations += ifBuildMessage(result, "cluster.alarms", {
            it
        }, "Please clear the alarms.")


        def textReport = buildTextReport(result)

        log.info(">>>>> ... cluster-audit finished")
        return new ClusterCheckResult(reportJson: result, reportText: textReport, recommendations: recommendations)
    }

    def buildTextReport(def result) {
        def text = ""
        for (def res in result) {
            def firstRun = true
            for (def vals in res.value) {
                if (!firstRun) {
                    text += "---\n"
                }
                text += "Hosts: ${vals['hosts']}\n"
                if (vals['value'] instanceof Collection) {
                    text += "${res.key} = \n"
                    for (def line in vals['value']) {
                        text += "> ${line}\n"
                    }
                } else {
                    text += "${res.key} = ${vals['value']}\n"
                }
                firstRun = false
            }
            text += "-----------------------------------------------------------------------\n"
        }
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

    def calculateRecommendationsForDiffValues(def result) {
        def propsWithSameValue = [
                "sysinfo.manufacturer",
                "sysinfo.product_name",
                "bios.vendor",
                "bios.version",
                "bios.release_date",
                "cpu.model",
                "cpu.architecture",
                "cpu.virtualization",
                "cpu.bogo_mips",
                "cpu.cores",
                "cpu.stepping",
                "cpu.byte_order",
                "cpu.cpu_mhz",
                "cpu.threads_per_core",
                "memory.total",
                "memory.swap_total",
                "memory.hugepage_total",
                "memory.dimm_slots",
                "memory.dimm_count",
                "memory.dimm_info",
                "ethernet.controller",
                "storage.controller",
                "storage.scsi_raid",
                "storage.udev_rules",
                "os.distribution",
                "os.kernel",
                "os.time",
                "os.umask",
                "os.repositories",
                "os.selinux",
                "os.locale",
                "os.packages.required",
                "os.kernel_params.vm.swappiness",
                "os.kernel_params.net.ipv4.tcp_retries2",
                "os.kernel_params.net.ipv4.tcp_fin_timeout",
                "os.kernel_params.vm.overcommit_memory",
                "os.thp",
                "storage.controller_max_transfer_size",
                "storage.controller_configured_transfer_size",
                "storage.tmp_dir_permission",
                "java.openjdk",
                "java.version",
                "java.version_output",
                "ulimit.mapr_processes",
                "ulimit.mapr_files",
                "ulimit.limits_conf",
                "mapr.system.user",
                "ulimit.limits_d_conf",
                "cluster.alarms"
        ]
        return propsWithSameValue.findAll {
            it -> result[it] != null && result[it].size() != 1
        }.collect { it ->
            return "'${it}' should have the same values for all nodes."
        }
    }

    private static def groupSameValuesWithHosts(def nodes) {
        def result = [:]
        for (def node : nodes) {
            def host = node['hostname']
            for (def e in node) {
                if (e.key != "hostname") {
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

    def parseInterfaceToMap(ifconfig, executeSudo) {
        def result = [:]
        def tokens = ifconfig.tokenize('\n')
        def ifName = ""
        def ifText = ""
        for (def token : tokens) {
            if (token.trim()) {
                if (!token.startsWith(" ")) {
                    if (ifName) {
                        result << getEthtool(ifName, executeSudo)
                        result['ethernet.interfaces.' + ifName + '.ifconfig'] = ifText.tokenize('\n')
                    }
                    def devTokens = token.tokenize(": ")
                    //    ifName = token.substring(0, token.indexOf(':')).trim()
                    if (devTokens.size() > 0) {
                        ifName = devTokens[0].trim()
                        ifText = token.substring(token.indexOf(':') + 1).trim()
                    }
                    else {
                        ifName = "notfound"
                        ifText = "not able to detect interface"
                    }
                } else {
                    ifText += '\n' + token.trim()
                }
            }
        }
        if (ifName) {
            result << getEthtool(ifName, executeSudo)
            result['ethernet.interfaces.' + ifName + '.ifconfig'] = ifText.tokenize('\n')
        }
        return result
    }

    def getEthtool(ifName, executeSudo) {
        def result = [:]
        def ethtool = executeSudo('/sbin/ethtool ' + ifName)
        result['ethernet.interfaces.' + ifName + '.speed'] = getColonValueFromLines(ethtool, "Speed:")
        result['ethernet.interfaces.' + ifName + '.duplex'] = getColonValueFromLines(ethtool, "Duplex:")
        return result
    }

    def getColonProperty(String memoryString, String property) {
        if (!memoryString) {
            return "not found"
        }
        def tokens = memoryString.tokenize('\n')
        def result = tokens.find { it.trim().startsWith(property) }
        return result ? getColonValue(result) : "not found"
    }

    def getColonValue(String line) {
        return line.substring(line.indexOf(':') + 1).trim()
    }

    def getColonValue(String line, String property) {
        return line.substring(line.indexOf(property) + property.length()).trim()
    }

    def getColonValueFromLines(String allLines, String property) {
        def line = findLine(allLines, property)
        if (!line) {
            return ""
        }
        return getColonValue(line, property)
    }

    def findLine(String allLines, String property) {
        def tokens = allLines.tokenize('\n')
        def result = tokens.find { it.trim().contains(property) }
        return result
    }

    def suStr(exec) {
        return "su ${globalYamlConfig.mapr_user} -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket;${exec}'"
    }

}
