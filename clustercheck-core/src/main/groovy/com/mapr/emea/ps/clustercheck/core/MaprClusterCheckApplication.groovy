package com.mapr.emea.ps.clustercheck.core

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import groovy.json.JsonOutput
import org.yaml.snakeyaml.Yaml

import java.text.SimpleDateFormat

@ComponentScan("com.mapr.emea.ps.clustercheck")
@SpringBootApplication
class MaprClusterCheckApplication implements CommandLineRunner {
    static final Logger log = LoggerFactory.getLogger(MaprClusterCheckApplication.class);

    @Autowired
    @Qualifier("globalYamlConfig")
    Map<String, ?> globalYamlConfig

    static String command;
    static String configFile;

    @Autowired
    ApplicationContext ctx;

    static void main(String[] args) {
        this.args = args;
        if(args.length != 2) {
            printHelpAndExit()
        }
            // TODO check commands
            // TODO check path
        else {
            SpringApplication.run MaprClusterCheckApplication, args
        }
    }

    private static void printHelpAndExit() {
        println "USAGE: "
        println "  Run checks:                      ./clusterchecksuite run /path/to/myconfig.yaml"
        println "  Validate configuration file:     ./clusterchecksuite validate /path/to/myconfig.yaml"
        println "  Create configuration template:   ./clusterchecksuite generatetemplate /path/to/myconfig.yaml"
        println ""
        System.exit(1)
    }

    @Bean("globalYamlConfig")
    static Map<String, ?> globalYamlConfig() {
        Yaml parser = new Yaml()
        return parser.load((configFile as File).text) as Map<String, ?>
    }

    @Override
    void run(String... args) throws Exception {
        // TODO implement validate
        // TODO implement run
        // TODO implement template
        def modules = ctx.getBeansWithAnnotation(ClusterCheckModule)
        log.info("Number of modules found: " + modules.size())
        log.info("====== Starting validation ======")
        int countErrors = 0
        for (Object module : modules.values()) {
            if(module instanceof ExecuteModule) {
                ExecuteModule m = (ExecuteModule)module
                def annotation = m.getClass().getAnnotation(ClusterCheckModule)
                log.info("Validating " + annotation.name() + " - " + annotation.version())
                try {
                    module.validate()
                }
                catch(ModuleValidationException ex) {
                    log.error("   " + ex.getMessage())
                    countErrors++;
                }
            }
            else {
                log.warn("Cannot validate module")
            }

        }
        log.info("====== Validation finished ======")
        if(countErrors > 0) {
            log.error("Validation occured, please fix them and re-run.")
            return
        }

        log.info("====== Starting execution =======")
        def sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
        def outputDir = new File(globalYamlConfig.outputDir + "/" + sdf.format(new Date()))
        if(outputDir.exists()) {
            log.error("Output directory already exists " + outputDir.getAbsolutePath())
            return
        }
        log.info("Creating output directory " + outputDir.getAbsolutePath())
        outputDir.mkdir()
        for (Object module : modules.values()) {
            if(module instanceof ExecuteModule) {
                ExecuteModule m = (ExecuteModule)module
                def annotation = m.getClass().getAnnotation(ClusterCheckModule)
                log.info("Executing " + annotation.name() + " - " + annotation.version())
                long moduleStart = System.currentTimeMillis()
                def result = m.execute()
                long moduleDurationInMs = System.currentTimeMillis() - moduleStart
                writeModuleJsonOutput(annotation, outputDir, result, moduleDurationInMs)
                writeModuleReportOutput(annotation, outputDir, result, moduleDurationInMs)
            }
            else {
                log.warn("Cannot execute module")
            }

        }
        // TODO
//        writeGlobalJsonOutput(annotation, outputDir, result)
//        writeGlobalReportOutput(annotation, outputDir, result)

        log.info("====== Execution finished ======")

    }

    def writeModuleJsonOutput(ClusterCheckModule annotation, File outputDir, ClusterCheckResult clusterCheckResult, long moduleDurationInMs) {
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def globalJson = [clusterName: globalYamlConfig.cluster_name, customerName: globalYamlConfig.customer_name, executionDate: sdf.format(new Date()), executionDurationInMs: moduleDurationInMs, executionHost: InetAddress.getLocalHost().getCanonicalHostName(), moduleName: annotation.name(), moduleVersion: annotation.version(), moduleResult: clusterCheckResult.reportJson, moduleRecommendations: clusterCheckResult.recommendations, configuration: globalYamlConfig]
        def json = JsonOutput.toJson(globalJson)
        def outputModuleDir = new File(outputDir.getAbsolutePath() + "/modules/" + annotation.name())
        outputModuleDir.mkdirs()
        def outputFile = new File(outputModuleDir.getAbsolutePath() + "/result.json")
        outputFile.text = JsonOutput.prettyPrint(json)
    }

    def writeModuleReportOutput(ClusterCheckModule annotation, File outputDir, ClusterCheckResult clusterCheckResult, long moduleDurationInMs) {
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def executedAt = sdf.format(new Date())
        def executionHost = InetAddress.getLocalHost().getCanonicalHostName()
        def outputHeader = """========================= REPORT INFO ====================================
Customer: ${globalYamlConfig.customer_name}
Cluster name: ${globalYamlConfig.cluster_name}
Executed at: ${executedAt}
Execution duration in ms: ${moduleDurationInMs}
Executed on host: ${executionHost}
Module name: ${annotation.name()}
Module version: ${annotation.version()}
========================= MODULE REPORT ==================================
"""
def recommendationHeader = """

========================= MODULE RECOMMENDATIONS =========================
"""
        def outputText = outputHeader + clusterCheckResult.reportText + recommendationHeader
        int i = 1
        for (String recommendation : clusterCheckResult.recommendations) {
            outputText += "----------------------- Recommendation ${i} ----------------\n"
            outputText += recommendation + "\n"
            i++
        }
        outputText += """
========================= CONFIGURATION ==================================
"""
        outputText += (configFile as File).text
        def outputModuleDir = new File(outputDir.getAbsolutePath() + "/modules/" + annotation.name())
        outputModuleDir.mkdirs()
        def outputFile = new File(outputModuleDir.getAbsolutePath() + "/report.txt")
        outputFile.text = outputText
    }

    def writeGlobalJsonOutput(ClusterCheckModule annotation, File outputDir, ClusterCheckResult clusterCheckResult) {
        def modulesResult = []
        def globalJson = [executionDate: "Date", executionDuration: "duration", executionHost: "hostname", modules: modulesResult]
        def json = JsonOutput.toJson(globalJson)
        def outputFile = new File(outputDir.getAbsolutePath() + "/result.json")
        outputFile.text = JsonOutput.prettyPrint(json)

    }
}
