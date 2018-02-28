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
// TODO implement TEXT report
// TODO implement diffs and give recommendations

// TODO grab pam conf
// TODO grab sssd conf
// TODO grab nscd conf


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
        return ['mapruser': 'mapr']
    }

    @Override
    List<String> validate() throws ModuleValidationException {
        def clusteraudit = globalYamlConfig.modules['cluster-audit'] as Map<String, ?>
        def role = clusteraudit.getOrDefault("role", "all")
        def warnings = []
        ssh.run {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                if (distribution.toLowerCase().contains("ubuntu")) {
                    def result = execute("dpkg -l pciutils dmidecode net-tools ethtool bind9utils > /dev/null || true").trim()
                    if(result) {
                        warnings << ("Please install following tools: " + result)
                    }

                } else {
                    def result = execute("rpm -q pciutils dmidecode net-tools ethtool bind-utils | grep 'is not installed' || true").trim()
                    if(result) {
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
        def mapruser = clusteraudit.getOrDefault("role", "mapr")
        def nodes = Collections.synchronizedList([])
        log.info(">>>>> Running cluster-audit")
        ssh.run {
            settings {
                pty = true
                ignoreError = true
            }
            session(ssh.remotes.role(role)) {
                def node = [:]
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
                node['cpu.cpus'] = getColonProperty(cpu, 'CPU(s)')
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
                node['memory.dimm_slots'] = execute('sudo dmidecode -t memory |grep -c \'^[[:space:]]*Locator:\'')
                node['memory.dimm_count'] = execute('sudo dmidecode -t memory | grep -c \'^[[:space:]]Size: [0-9][0-9]*\'')
                node['memory.dimm_info'] = execute('sudo dmidecode -t memory | awk \'/Memory Device$/,/^$/ {print}\'')

                // NIC / Ethernet

                def executeSudo = { arg -> executeSudo(arg) }
                def ethernetControllers = findLine(executeSudo('lspci'), 'Ethernet controller:')
                def ifconfig = executeSudo('ifconfig -a')
                def ifconfigMap = parseInterfaceToMap(ifconfig, executeSudo)
                def distribution = execute("[ -f /etc/system-release ] && cat /etc/system-release || cat /etc/os-release | uniq")
                def systemd = execute("[ -f /etc/systemd/system.conf ] && echo true || echo false")
                node['ethernet.controller'] = getColonProperty(ethernetControllers, "Ethernet controller:")
                node['ethernet.interfaces'] = ifconfigMap
                node['storage.controller'] = executeSudo("lspci | grep -i -e ide -e raid -e storage -e lsi")
                node['storage.scsi_raid'] = executeSudo("dmesg | grep -i raid | grep -i -o 'scsi.*\$' | uniq")
                node['storage.disks'] = executeSudo("fdisk -l | grep '^Disk /.*:' |sort")
                node['storage.udev_rules'] = executeSudo("ls /etc/udev/rules.d")
                node['os.distribution'] = distribution
                node['os.kernel'] = execute("uname -srvmo | fmt")
                node['os.time'] = execute("date")
                node['os.umask'] = executeSudo("umask")
                node['os.locale'] = getColonValueFromLines(executeSudo("su - ${mapruser} -c 'locale | grep LANG'"), "LANG=")

                if (distribution.toLowerCase().contains("ubuntu")) {
                    node['os.services.ntpd'] = executeSudo("service ntpd status || true")
                    node['os.services.apparmor'] = executeSudo("apparmor_status | sed 's/([0-9]*)//' || true")
                    node['os.services.selinux'] = execute("([ -d /etc/selinux -a -f /etc/selinux/config ] && grep ^SELINUX= /etc/selinux/config) || echo 'Disabled'")
                    node['os.services.firewall'] = executeSudo("service ufw status | head -10 || true")
                    node['os.services.iptables'] = executeSudo("iptables -L | head -10 || true")
                    node['os.packages.nfs'] = execute("dpkg -l '*nfs*' | grep ^i")
                } else {
                    if (distribution.toLowerCase().contains("sles")) {
                        node['os.repositories'] = execute("zypper repos | grep -i mapr")
                        node['os.selinux'] = execute("rpm -q selinux-tools selinux-policy")
                        node['os.firewall'] = executeSudo("service SuSEfirewall2_init status")
                    } else {
                        node['os.repositories'] = executeSudo("yum --noplugins repolist | grep -i mapr ")
                        node['os.selinux'] = executeSudo("getenforce")

                    }
                    node['os.packages.nfs'] = execute("rpm -qa | grep -i nfs | sort")
                    node['os.packages.required'] = execute("rpm -q dmidecode bind-utils irqbalance syslinux hdparm sdparm rpcbind nfs-utils redhat-lsb-core ntp | grep 'is not installed' || true")
                    node['os.packages.optional'] = execute("rpm -q patch nc dstat xml2 jq git tmux zsh vim nmap mysql mysql-server tuned smartmontools pciutils lsof lvm2 iftop ntop iotop atop ftop htop ntpdate tree net-tools ethtool | grep 'is not installed' || true")
                }

                if (systemd == "true") {
                    node['os.services.ntpd'] = executeSudo("systemctl status ntpd || true")
                    node['os.services.sssd'] = executeSudo("systemctl status sssd || true")
                    node['os.services.firewall'] = executeSudo("systemctl status firewalld || true")
                    node['os.services.iptables'] = executeSudo("systemctl status iptables || true")
                    node['os.services.cpuspeed'] = executeSudo("systemctl status cpuspeed || true")

                } else {
                    node['os.services.ntpd'] = executeSudo("service ntpd status || true")
                    node['os.services.sssd'] = executeSudo("service sssd status || true")
                    node['os.services.iptables'] = executeSudo("service iptables status | head -10 || true")
                    node['os.services.cpuspeed'] = executeSudo("chkconfig --list cpuspeed || true")
                }

                node['os.kernel_params.vm.swappiness'] = getColonValue(executeSudo("sysctl vm.swappiness"), '=')
                node['os.kernel_params.net.ipv4.tcp_retries2'] = getColonValue(executeSudo("sysctl net.ipv4.tcp_retries2"), '=')
                node['os.kernel_params.vm.overcommit_memory'] = getColonValue(executeSudo("sysctl vm.overcommit_memory"), '=')
                node['os.thp'] = executeSudo("cat /sys/kernel/mm/transparent_hugepage/enabled")
                node['storage.luks'] = executeSudo("grep -v -e ^# -e ^\$ /etc/crypttab | uniq -c -f2 || true")
                node['storage.controller_max_transfer_size'] = executeSudo("files=\$(ls /sys/block/{sd,xvd,vd}*/queue/max_hw_sectors_kb 2>/dev/null); for each in \$files; do printf \"%s: %s\\n\" \$each \$(cat \$each); done |uniq -c -f1")
                node['storage.controller_configured_transfer_size'] = executeSudo("files=\$(ls /sys/block/{sd,xvd,vd}*/queue/max_sectors_kb 2>/dev/null); for each in \$files; do printf \"%s: %s\\n\" \$each \$(cat \$each); done |uniq -c -f1")
                if (systemd == "true") {
                    node['storage.mounted_fs'] = executeSudo("df -h --output=fstype,size,pcent,target -x tmpfs -x devtmpfs")
                } else {
                    node['storage.mounted_fs'] = executeSudo("df -hT | cut -c22-28,39- | grep -e '  *' | grep -v -e /dev")
                }
                node['storage.mount_permissions'] = executeSudo("mount | grep -e noexec -e nosuid | grep -v tmpfs |grep -v 'type cgroup'").tokenize('\n')
                node['storage.tmp_dir_permission'] = executeSudo("stat -c %a /tmp")
                def java_version_output = execute("java -version")
                node['java.openjdk'] = java_version_output.toLowerCase().contains("openjdk")
                node['java.version'] = getColonValueFromLines(java_version_output, "version").replace('"', '')
                node['java.version_output'] = java_version_output

                if (distribution.toLowerCase().contains("sles")) {
                    node['ip'] = execute('hostname -i')
                } else {
                    node['ip'] = execute('hostname -I')
                }
                node['ulimit.mapr_processes'] = executeSudo("su - ${mapruser} -c 'ulimit -u'")
                node['ulimit.mapr_files'] = executeSudo("su - ${mapruser} -c 'ulimit -n'")
                node['ulimit.limits_conf'] = executeSudo("grep -e nproc -e nofile /etc/security/limits.conf |grep -v ':#'")
                node['ulimit.limits_d_conf'] = executeSudo("[ -d /etc/security/limits.d ] && (grep -e nproc -e nofile /etc/security/limits.d/*.conf |grep -v ':#')")


                node['dns.lookup'] = execute("host " + node['hostname'])
                node['dns.reverse'] = execute("host " + node['ip'])

                nodes.add(node)
            }
        }
        def result = groupSameValuesWithHosts(nodes)
        // TODO apply recommendations here
        // TODO generic one for different values
        // TODO recommendation ulimit
        // TODO recommendation THP
        // TODO recommendation kernel params
        // TODO recommendation os required packages
        // TODO recommendation umask
        // TODO recommendation locale
        // TODO recommendation dns, check for "has address"
        // TODO recommendation reverse dns, check for "domain name pointer"


        log.info(">>>>> ... cluster-audit finished")
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
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
                        result[ifName] = getEthtool(ifName, executeSudo)
                        result[ifName]['ifconfig'] = ifText
                    }
                    ifName = token.substring(0, token.indexOf(':')).trim()
                    ifText = token.substring(token.indexOf(':') + 1).trim()
                } else {
                    ifText += '\n' + token.trim()
                }
            }
        }
        if (ifName) {
            result[ifName] = getEthtool(ifName, executeSudo)
            result[ifName]['ifconfig'] = ifText
        }
        return result
    }

    def getEthtool(ifName, executeSudo) {
        def result = [:]
        def ethtool = executeSudo('/sbin/ethtool ' + ifName)
        result['speed'] = getColonValueFromLines(ethtool, "Speed:")
        result['duplex'] = getColonValueFromLines(ethtool, "Duplex:")
        return result
    }

    def getColonProperty(String memoryString, String property) {
        if(!memoryString) {
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

}
