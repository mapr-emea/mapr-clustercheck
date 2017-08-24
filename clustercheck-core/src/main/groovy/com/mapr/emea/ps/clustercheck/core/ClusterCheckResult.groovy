package com.mapr.emea.ps.clustercheck.core

/**
 * Created by chufe on 22.08.17.
 */
// TODO add files list and persist files in module folder (e.g. for yarn-site.xml)
class ClusterCheckResult {
    Object reportJson; // set Map or List
    String reportText;
    List<String> recommendations = []
}
