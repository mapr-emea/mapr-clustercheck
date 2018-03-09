package com.mapr.emea.ps.clustercheck.core

/**
 * Created by chufe on 22.08.17.
 */
class ModuleInternalResult {
    ClusterCheckResult result
    ClusterCheckModule module
    long moduleDurationInMs
    String executedAt
    String runId
}
