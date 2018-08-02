package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.extensions;

import com.google.common.base.Splitter;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by miyurud on 4/21/15.
 */
public class MostFrequentWordFunction extends FunctionExecutor {
    private final String NONE = "NONE";
    private final String COLON = ":";


    protected void init(ExpressionExecutor[] attributeExpressionExecutors, SiddhiAppContext executionPlanContext) {

    }

    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] data) {
        String body = data[0].toString();
        String subject = data[1].toString();

        String mostFrequentWord = getMostFrequentWord(body);
        subject = mostFrequentWord + COLON + subject;
        return subject;


        //return "abc";
    }

    @Override
    protected Object execute(Object data) {
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


    private String getMostFrequentWord(String body){
        Splitter splitter = Splitter.on(' ');
        Iterator<String> dataStrIterator = splitter.split(body.toLowerCase()).iterator();
        HashMap<String, Integer> index = new HashMap<String, Integer>();
        String mostFrequentWord = NONE;
        int mostFrequentCount = 1;

        while(dataStrIterator.hasNext()){
            String word = dataStrIterator.next().trim();

            if(word.length() > 0) {
                Integer count = index.get(word);

                if (count == null) {
                    index.put(word, 1);
                } else {
                    ++count;
                    if( count.intValue() > mostFrequentCount) {
                        mostFrequentCount = count;
                        mostFrequentWord = word;
                    }
                    index.put(word, count);
                }
            }
        }

        return mostFrequentWord;
    }


    /**
     * This method return "NONE" if there is no frequent word in the body. This may be due to all the words appear only
     * once in the email's body or due to a single letter becomming the most frequent item.
     * @param body
     * @return
     */
    private String getMostFrequentWordOLD(String body){
        Splitter splitter = Splitter.on(' ');
        Iterator<String> dataStrIterator = splitter.split(body).iterator();
        HashMap<String, Integer> index = new HashMap<String, Integer>();

        while(dataStrIterator.hasNext()){
            String word = dataStrIterator.next().toLowerCase().trim();

            if(word.length() > 0) {
                Integer count = index.get(word);

                if (count == null) {
                    index.put(word, 1);
                } else {
                    index.put(word, ++count);
                }
            }
        }

        Iterator<Map.Entry<String, Integer>> itr = index.entrySet().iterator();

        Map.Entry<String, Integer> mostFrequentItem = new Map.Entry<String, Integer>() {
            public String getKey() {
                return null;
            }

            public Integer getValue() {
                return 0;
            }

            public Integer setValue(Integer value) {
                return null;
            }
        };

        while(itr.hasNext()){
            Map.Entry<String, Integer> item = itr.next();
            if(item.getValue() > mostFrequentItem.getValue()){
                mostFrequentItem = item;
            }
        }

        if(mostFrequentItem.getValue() == 1){
            return NONE;
        }else if(mostFrequentItem.getKey().length() == 1) {
            return NONE;
        }else {
            return mostFrequentItem.getKey();
        }
    }
}
