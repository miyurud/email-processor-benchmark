package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.extensions;

import com.google.common.base.Splitter;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by miyurud on 4/17/15.
 */
public class MetricsFunction  extends FunctionExecutor {
    private long emailCounter;
    private long windowDuration = 10000;//time is in ms
    private long windowStartTime;
    private String COMMA = ",";


    protected void init(ExpressionExecutor[] attributeExpressionExecutors, SiddhiAppContext executionPlanContext) {

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
        long characterCounter = 0;
        long wordCounter = 0;
        long paragraphCounter = 0;

        String body = data.toString();

        //The following is for words and characters
        Splitter splitter = Splitter.on(' ');
        Iterator<String> itr = splitter.split(body).iterator();
        String word = null;


        while(itr.hasNext()){
            word = itr.next();

            //Note that we are not considering letter 'a' as a word.
            int numChars = word.length();

            if( numChars > 1){
                wordCounter++;
            }

            characterCounter += numChars;
        }

        //The following is for paragraphs
        splitter = Splitter.on("\n\n");
        itr = splitter.split(body).iterator();

        while(itr.hasNext()){
            itr.next();
            paragraphCounter++;
        }

        return characterCounter + COMMA + wordCounter + COMMA + paragraphCounter;
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
