package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.extensions;

import com.google.common.base.Splitter;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by miyurud on 4/17/15.
 */
public class GlobalMetricsFunction extends FunctionExecutor {
    private long emailCounter;
    private long characterCounter = 0;
    private long wordCounter = 0;
    private long paragraphCounter = 0;

    private long windowDuration = 10000;//time is in ms
    private long windowStartTime;
    private int LOGGING_WINDOW_IN_MS=5000;//This is in miliseconds
    private File metricsLogFile;
    private String LOG_FILE_PATH = "/home/cep/miyurud/tmp/cepserver/email-metrics";
    private BufferedWriter bw;
    private long prevTimeStamp;
    private static String COMMA = ",";
    private long startTime = 0;
    private boolean firstFlag = true;


    protected void init(ExpressionExecutor[] attributeExpressionExecutors, SiddhiAppContext executionPlanContext) {
        java.util.Date date= new java.util.Date();
        String tt = (new Timestamp(date.getTime())).toString().replace(' ', '-');
        metricsLogFile = new File(LOG_FILE_PATH + "-" + tt + ".txt");

        try {
            if (!metricsLogFile.exists()) {
                metricsLogFile.createNewFile();
            }

            FileWriter fw = new FileWriter(metricsLogFile.getAbsoluteFile());
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        prevTimeStamp = System.currentTimeMillis();
    }

    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] data) {
        return null;
    }

    /**
     * This bolt gathers only email level attributes. The Aggregated results are calculated at the GlobalMetricsFunction
     * @param data
     * @return
     */
    @Override
    protected Object execute(Object data) {
        long currTime = System.currentTimeMillis();

        if(firstFlag){
            firstFlag = false;
            startTime = currTime;
        }

        emailCounter++;
        String body = data.toString();

        //The following is for words and characters
        Splitter splitter = Splitter.on(COMMA);
        Iterator<String> itr = splitter.split(body).iterator();
        String word = null;

        while(itr.hasNext()){
            characterCounter += Long.parseLong(itr.next());
            wordCounter += Long.parseLong(itr.next());
            paragraphCounter += Long.parseLong(itr.next());
        }

        if((currTime - prevTimeStamp) > LOGGING_WINDOW_IN_MS){
            try {
                bw.write((currTime - startTime) + COMMA + emailCounter + COMMA + characterCounter + COMMA + wordCounter + COMMA + paragraphCounter);
                bw.newLine();
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            prevTimeStamp = currTime;
        }

        return null;
    }

    public void start() {

    }

    public void stop() {

    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {

    }

    public void restoreState(Object[] state) {

    }
}
