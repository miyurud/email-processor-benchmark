package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.extensions;

import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by miyurud on 4/17/15.
 */

public class FilterFunction  extends FunctionExecutor {
    private String ENRON_DOMAIN = "enron.com";
    private static final Logger logger = Logger.getLogger(FilterFunction.class);


    protected void init(ExpressionExecutor[] attributeExpressionExecutors, SiddhiAppContext executionPlanContext) {

    }

    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }


    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] data) {
        boolean flag = false;
        List<CharSequence> addressesList = (List<CharSequence>)(data[0]);
        List<CharSequence> addressesListNew = null;

        //To addresses
        if(addressesList != null){
            addressesListNew = new LinkedList<CharSequence>();
            Iterator<CharSequence> itr = addressesList.iterator();

            CharSequence item = null;

            //We have to iterate through the To email addresses and see whether those contain any email address which is
            //outside the "enron.com" domain. If so we have to remove that email address.

            while (itr.hasNext()) {
                item = itr.next();
                //logger.info("-->" + item);

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    addressesListNew.add(item.toString());
                } else {
                    flag = true;
                }
            }
        }
        //else{
            //logger.info("Address list is null");
        //}

        return addressesListNew;
    }

    @Override
    protected Object execute(Object data) {
        boolean flag = false;

        String commaSeparatedAddresses = (String)data;
        //logger.info("commaSeparatedAddresses: " + commaSeparatedAddresses);
        Splitter splitter = Splitter.on(',');
        Iterator<String> dataStrIterator = splitter.split(commaSeparatedAddresses).iterator();
        String emailAddress = null;
        StringBuilder sb = new StringBuilder();

        while(dataStrIterator.hasNext()){
            emailAddress = dataStrIterator.next();
            //logger.info("emailAddress: " + emailAddress);
            if(emailAddress.contains(ENRON_DOMAIN)){
                sb.append(emailAddress);
            }
        }

        String result = sb.toString().trim();

        if(result.length() > 0) {
            //logger.info("result: " + result);
            return result;
        }else{
            //logger.info("result is empty");
            return "";
        }
    }

//    private boolean isValid(String emailAddress) {
//        if()
//
//        return false;
//    }

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
