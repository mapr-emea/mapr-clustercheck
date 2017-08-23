package com.mapr.emea.ps.clustercheck.module.clusteraudit

import com.mapr.emea.ps.clustercheck.core.ClusterCheckModule
import com.mapr.emea.ps.clustercheck.core.ClusterCheckResult
import com.mapr.emea.ps.clustercheck.core.ExecuteModule
import com.mapr.emea.ps.clustercheck.core.ModuleValidationException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * Created by chufe on 22.08.17.
 */
@ClusterCheckModule(name = "clusteraudit", version = "1.0")
class ClusterAuditModule implements ExecuteModule {
    @Autowired
    @Qualifier("ssh")
    def ssh
    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    @Override
    void validate() throws ModuleValidationException {
        // TODO implement check with hostname
    }

    @Override
    ClusterCheckResult execute() {
        def clusteraudit = globalYamlConfig.modules.clusteraudit as Map<String, ?>
        def role = clusteraudit.getOrDefault("role", "all")
        def result = Collections.synchronizedList([])

        ssh.run {
            settings {
                pty = true
            }

            session(ssh.remotes.role(role)) {
                def node = [:]
                node['hostname'] = execute 'hostname -f'

                // Sysinfo
                def dmicodeSysInfo = execute('sudo dmidecode | grep -A2 \'^System Information\'')
                node['sysinfo'] = [:]
                node['sysinfo']['manufacturer'] = getColonProperty(dmicodeSysInfo, "Manufacturer")
                node['sysinfo']['product_name'] = getColonProperty(dmicodeSysInfo, "Product Name")

                // BIOS
                def bios = execute('sudo dmidecode | grep -A3 \'^BIOS I\'')
                node['bios'] = [:]
                node['bios']['vendor'] = getColonProperty(bios, "Vendor")
                node['bios']['version'] = getColonProperty(bios, "Version")
                node['bios']['release_Date'] = getColonProperty(bios, "Release Date")

                // CPU
                def cpu = execute('lscpu')
                node['cpu'] = [:]
                node['cpu']['model'] = getColonProperty(cpu, 'Model name')
                node['cpu']['architecture'] = getColonProperty(cpu, 'Architecture')
                node['cpu']['virtualization'] = getColonProperty(cpu, 'Virtualization type')
                node['cpu']['bogo_mips'] = getColonProperty(cpu, 'BogoMIPS')
                node['cpu']['cpus'] = getColonProperty(cpu, 'CPU(s)')
                node['cpu']['stepping'] = getColonProperty(cpu, 'Stepping')
                node['cpu']['byte_order'] = getColonProperty(cpu, 'Byte Order')
                node['cpu']['cpu_mhz'] = getColonProperty(cpu, 'CPU MHz')
                node['cpu']['threads_per_core'] = getColonProperty(cpu, 'Thread(s) per core')

                // Memory
                def memory = execute('cat /proc/meminfo')
                node['memory'] = [:]
                node['memory']['total'] = getColonProperty(memory, "MemTotal")
                node['memory']['free'] = getColonProperty(memory, "MemFree")
                node['memory']['swap_cached'] = getColonProperty(memory, "SwapCached")
                node['memory']['swap_total'] = getColonProperty(memory, "SwapTotal")
                node['memory']['swap_free'] = getColonProperty(memory, "SwapFree")
                node['memory']['hugepage_total'] = getColonProperty(memory, "HugePages_Total")
                node['memory']['hugepage_free'] = getColonProperty(memory, "HugePages_Free")
                node['memory']['dimm_slots'] = execute('sudo dmidecode -t memory |grep -c \'^[[:space:]]*Locator:\'')
                node['memory']['dimm_count'] = execute('sudo dmidecode -t memory | grep -c \'^[[:space:]]Size: [0-9][0-9]*\'')
                node['memory']['dimm_info'] = execute('sudo dmidecode -t memory | awk \'/Memory Device$/,/^$/ {print}\'')

                // NIC / Ethernet
                def lspci = dropEverythingBeforeString(execute('sudo lspci'), ' ')
                def ifconfig = execute('sudo ifconfig -a')
                node['ethernet'] = [:]
                node['ethernet']['controller'] = getColonProperty(lspci, "Ethernet controller")
                node['ethernet']['interfaces'] = []
                // mtu size
                // dropped packets, errors
                // ip + mask
//sudo /sbin/ethtool eth0

/*
Settings for eth0:
        Supported ports: [ ]
        Supported link modes:   10000baseT/Full
        Supported pause frame use: No
        Supports auto-negotiation: No
        Advertised link modes:  Not reported
        Advertised pause frame use: No
        Advertised auto-negotiation: No
        Speed: 10000Mb/s
        Duplex: Full
        Port: Other
        PHYAD: 0
        Transceiver: Unknown!
        Auto-negotiation: off
        Current message level: 0x00000007 (7)
                               drv probe link
        Link detected: yes
 */

                result.add(node)
            }
        }
        return new ClusterCheckResult(reportJson: result, reportText: "Not yet implemented", recommendations: ["Not yet implemented"])
//        return [firstName:'John', lastName:'Doe', age:25]
    }

    def getColonProperty(String memoryString, String memoryProperty) {
        def tokens = memoryString.tokenize('\n')
        def result = tokens.find { it.trim().startsWith(memoryProperty) }
        return result ? getColonValue(result) : "Not found"
    }

    def getColonValue(String line) {
        return line.substring(line.indexOf(':') + 1).trim()
    }

    def dropEverythingBeforeString(String allLines, String sep) {
        def tokens = allLines.tokenize('\n')
        def removed = tokens.collect {
            it.substring(it.indexOf(sep) + 1).trim()
        }
        return removed.join('\n')
    }

}
