package com.mapr.emea.ps.clustercheck.core

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.ComponentScan

@ComponentScan("com.mapr.emea.ps.clustercheck")
@SpringBootApplication
class MaprClusterCheckApplication implements CommandLineRunner {
    @Autowired
    ApplicationContext ctx;

    static void main(String[] args) {
        SpringApplication.run MaprClusterCheckApplication, args
    }

    @Override
    void run(String... args) throws Exception {
        def modules = ctx.getBeansWithAnnotation(ClusterCheckModule)
        println "Number of modules found: " + modules.size()
        for (Object module : modules.values()) {
            if(module instanceof ExecuteModule) {
                ExecuteModule m = (ExecuteModule)module
                println m.execute()
            }
            else {
                println "Cannot execute module"
            }

        }
    }
}
