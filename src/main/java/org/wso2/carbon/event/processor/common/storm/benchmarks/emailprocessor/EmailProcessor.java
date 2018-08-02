package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.uebercomputing.mailrecord.MailRecord;
import org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by miyurud on 4/9/15.
 */
public class EmailProcessor {
    private static LinkedBlockingQueue<Object> eventBufferList = null;
<<<<<<< HEAD
    private static String inputfilePath = "/home/sarangan/Downloads/enron.avro";
=======
    private static String inputfilePath = "/home/miyurud/Projects/CEPStormPerf/EmailProcessorBenchmark/datasets/Enron/Avro/enron.avro";
>>>>>>> c5799b2760711c3932d610cb5fc348c82d29aac4
    public static void main(String[] args){
        eventBufferList = new LinkedBlockingQueue<Object>(Constants.EVENT_BUFFER_SIZE);

        FilterOperator filterOperator = new FilterOperator();
        MetricsOperator metricsOperator = new MetricsOperator();
        ModifyOperator modifyOperator = new ModifyOperator();

        DataLoderThread dataLoderThread = new DataLoderThread(inputfilePath, eventBufferList);
        dataLoderThread.start();

        while(true){
            try {
                MailRecord obj = (MailRecord)eventBufferList.take();

                obj = filterOperator.process(obj);

                if(obj != null) {
                    obj = modifyOperator.process(obj);
                    obj = metricsOperator.process(obj);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
