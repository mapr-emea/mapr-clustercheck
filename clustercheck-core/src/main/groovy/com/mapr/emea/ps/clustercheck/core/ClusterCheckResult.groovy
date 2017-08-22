package com.mapr.emea.ps.clustercheck.core

/**
 * Created by chufe on 22.08.17.
 */
class ClusterCheckResult {
    Object reportJson; // set Map or List
    String reportText;
    List<String> recommandations = []
}
