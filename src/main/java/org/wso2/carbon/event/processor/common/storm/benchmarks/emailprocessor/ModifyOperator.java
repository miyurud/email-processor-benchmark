package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.google.common.base.Splitter;
import com.uebercomputing.mailrecord.MailRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by miyurud on 4/16/15.
 */
public class ModifyOperator {
    private String[] keyPeopleArray = new String[]{"Kenneth Lay","Jeffrey Skilling","Andrew Fastow"};
    private String[] replacementNamesArray = new String[]{"Person1","Person2","Person3"};

    public MailRecord process(MailRecord obj) {
        String body = obj.getBody().toString();

        //First we have to find the three key people from the Enron email database and replace the three names with
        //representative names.
        int keyPeopleArrayLen = keyPeopleArray.length;

        for(int i=0; i<keyPeopleArrayLen; i++){
            body = body.replace(keyPeopleArray[i], replacementNamesArray[i]);
        }

        obj.setBody(body);

        //Next, we need to find the most frequent word from the email body and prepend it to the email's title.
        String mostFrequentWord = getMostFrequentWord(body);
        obj.setSubject(mostFrequentWord + ":" + obj.getSubject());

        return obj;
    }

    private String getMostFrequentWord(String body){
        body = body.toLowerCase();
        Splitter splitter = Splitter.on(' ');
        Iterator<String> dataStrIterator = splitter.split(body).iterator();
        HashMap<String, Integer> index = new HashMap<String, Integer>();

        while(dataStrIterator.hasNext()){
            String word = dataStrIterator.next();

            Integer count = index.get(word);

            if(count == null){
                index.put(word, 1);
            }else {
                index.put(word, ++count);
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

        return mostFrequentItem.getKey();
    }
}
