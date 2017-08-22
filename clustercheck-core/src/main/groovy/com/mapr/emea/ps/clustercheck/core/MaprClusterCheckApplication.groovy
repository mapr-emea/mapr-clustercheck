package com.mapr.emea.ps.clustercheck.core

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import groovy.json.JsonOutput
import org.yaml.snakeyaml.Yaml

@ComponentScan("com.mapr.emea.ps.clustercheck")
@SpringBootApplication
class MaprClusterCheckApplication implements CommandLineRunner {
    static final Logger log = LoggerFactory.getLogger(MaprClusterCheckApplication.class);

    static String[] args;

    @Autowired
    ApplicationContext ctx;

    static void main(String[] args) {
        this.args = args;
        SpringApplication.run MaprClusterCheckApplication, args
    }

    @Bean("globalYamlConfig")
    static Map<String, ?> globalYamlConfig() {
        if(args.length != 1) {
            log.error("Usage: ./clusterchecksuite myconfig.yaml")
            System.exit(1)
        }
        Yaml parser = new Yaml()
        return parser.load((args[0] as File).text) as Map<String, ?>
    }

    @Override
    void run(String... args) throws Exception {
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
        for (Object module : modules.values()) {
            if(module instanceof ExecuteModule) {
                ExecuteModule m = (ExecuteModule)module
                def annotation = m.getClass().getAnnotation(ClusterCheckModule)
                log.info("Executing " + annotation.name() + " - " + annotation.version())
                def result = m.execute()
                def file = new File("/Users/chufe/testen.txt")
                def json = JsonOutput.toJson(result)
                file.text = JsonOutput.prettyPrint(json)
            }
            else {
                log.warn("Cannot execute module")
            }

        }
        log.info("====== Execution finished ======")

    }
}
