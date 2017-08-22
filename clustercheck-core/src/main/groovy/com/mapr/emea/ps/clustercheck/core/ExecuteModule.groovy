package com.mapr.emea.ps.clustercheck.core

/**
 * Created by chufe on 22.08.17.
 */
interface ExecuteModule {
    void validate() throws ModuleValidationException
    ClusterCheckResult execute()
}