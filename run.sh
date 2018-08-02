!/bin/bash
clear

java -cp /home/sarangan/Downloads/Processor_email/target/CEPStormPerf-1.0-SNAPSHOT-jar-with-dependencies.jar:lib/kafka-clients-0.11.0.0.jar org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.EmailProcessor
