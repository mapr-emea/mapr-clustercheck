#!/usr/bin/env groovy

@Grab('org.yaml:snakeyaml:1.18')
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml

def nodes = []

new File("/Users/chufe/daimler_hosts").eachLine {
    line ->
        def tokens = line.tokenize(' ')
        def hostname = tokens[1]
        def role = tokens[3]
        nodes << [host: hostname, ssh_user: "ansible", ssh_identity: "/home/ansible/.ssh/id_rsa", roles: [role]]
}


def dumperOptions1 = new DumperOptions();
dumperOptions1.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
Yaml yaml = new Yaml(dumperOptions1)
String output = yaml.dump([nodes: nodes])
new File("/Users/chufe/daimler_hosts.yml").text = output