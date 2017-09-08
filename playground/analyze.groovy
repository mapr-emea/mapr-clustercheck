#!/usr/bin/env groovy

@Grab('org.yaml:snakeyaml:1.18')
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml

def nodes = []
def prodStr = """
smtcaz1014 smtcaz1021 smtcaz1005 smtcaz1007 smtcaz1009
smtcaz1010 smtcaz1004 smtcaz1019 smtcaz1015 smtcaz1011 smtcaz1025
smtcaz1023 smtcaz1012 smtcaz1006 smtcaz1013 smtcaz1020 smtcaz1038
smtcaz1027 smtcaz1027 smtcaz1028 smtcaz1018 smtcaz1040 smtcaz1029
smtcaz1039 smtcaz1035 smtcaz1037 smtcaz1024 smtcaz1022 smtcaz1026
smtcaz1033 smtcaz1030 smtcaz1046 smtcaz1052 smtcaz1053 smtcaz1064
smtcaz1042 smtcaz1054 smtcaz1044 smtcaz1058 smtcaz1045 smtcaz1066
smtcaz1041 smtcaz1059 smtcaz1057 smtcaz1008 smtcaz1008 smtcaz1034 smtcaz1034
"""

//.rd.corpintra.net
//new File("/Users/chufe/daimler_hosts").eachLine {
//    line ->
//        def tokens = line.tokenize(' ')
//        def hostname = tokens[1]
//        def role = tokens[3]
//        nodes << [host: hostname, ssh_user: "root", ssh_identity: "/root/.ssh/id_rsa", roles: [role]]
//}

prodStr.tokenize().forEach{
    hostname ->
        //        def tokens = line.tokenize(' ')
        nodes << [host: hostname.trim()+  ".rd.corpintra.net", ssh_user: "root", ssh_identity: "/root/.ssh/id_rsa"]
}

def dumperOptions1 = new DumperOptions();
dumperOptions1.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
Yaml yaml = new Yaml(dumperOptions1)
String output = yaml.dump([nodes: nodes])
new File("/Users/chufe/Documents/workspaces/daimler-review/computacenta/daimler_prod_hosts.yml").text = output
// /Users/chufe/Documents/workspaces/daimler-review/dxc/audit/memory-test.log