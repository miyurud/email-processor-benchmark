package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.google.common.base.Splitter;
import com.uebercomputing.mailrecord.MailRecord;

import java.util.Iterator;

/**
 * Created by miyurud on 4/16/15.
 */
public class MetricsOperator {
    private long emailCounter;
    private long wordCounter;
    private long charcterCounter;
    private long windowDuration = 10000;//time is in ms
    private long windowStartTime;
    //We keep the GlobalMetricsOperator here for a short while
    private GlobalMetricsOperator globalMetricsOperator = new GlobalMetricsOperator();

    public MailRecord process(MailRecord obj) {
        //Count the total number of emails
        emailCounter++;

        String body = obj.getBody().toString();

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

            charcterCounter += numChars;
        }

        //Count the number of characters, words, and paragraphs in the email body processed
        //in a running window.
        long currentTime = System.currentTimeMillis();

        if((currentTime - windowStartTime) >= windowDuration){
            windowStartTime = currentTime;
            globalMetricsOperator.process(emailCounter, wordCounter, charcterCounter);
        }

        return obj;
    }
}
