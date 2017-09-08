#!/usr/bin/env groovy

def nodes = []

new File("/Users/chufe/Documents/workspaces/daimler-review/computacenta/clusteraudit/cluster_memory_maprprod.log").eachLine {
    line ->
        if(line.startsWith("Triad:")) {
            println line
        }
}


//