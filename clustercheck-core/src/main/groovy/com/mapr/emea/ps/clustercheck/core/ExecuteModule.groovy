package com.mapr.emea.ps.clustercheck.core

/**
 * Created by chufe on 22.08.17.
 */
interface ExecuteModule {
    /**
     * This is used when executing 'generatetemplate'. This takes the properties and adds it to the template.
     *
     * @return sample list for yaml configuration properties for module
     */
    Map<String, ?> yamlModuleProperties()
    /**
     * Executes the validation and checks for given preconditions for the module.
     * If this fails, no test will run. This blocks all modules.
     *
     * @throws ModuleValidationException blocks further execution
     */
    List<String> validate() throws ModuleValidationException
    /**
     * Executes the cluster check for the module.
     *
     * @return result of module execution
     */
    ClusterCheckResult execute()
}