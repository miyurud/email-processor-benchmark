package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

/**
 * Created by miyurud on 4/17/15.
 */
public class GlobalMetricsOperator {

    public void process(long emailCounter, long wordCounter, long characterCounter) {
        System.out.println("emailCounter:" + emailCounter + ", wordCounter:" + wordCounter + ", characterCounter:" + characterCounter);
    }
}
