package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.extensions;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import javax.mail.internet.MimeUtility;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by miyurud on 4/17/15.
 */
public class ModifyFunction extends FunctionExecutor {
    private String[] keyPeopleArray = new String[]{"Kenneth Lay","Jeffrey Skilling","Andrew Fastow"};
    private String[] replacementNamesArray = new String[]{"Person1","Person2","Person3"};


    protected void init(ExpressionExecutor[] attributeExpressionExecutors, SiddhiAppContext executionPlanContext) {

    }

    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] data) {
        return null;
    }

    @Override
    protected Object execute(Object data) {
        String body = data.toString();

        //-------- task 3 ----------------------------------------------------------------------------------------------
        //we need to remove rogue formatting such as dangling newline characters
        //and MIME quoted-printable characters
        //from the email body to restrict the character set to simple ASCII

        //remove dangling new lines
        body = body.trim();

        //Remove any MIME encoded text.
        try {
            body = MimeUtility.decodeText(body);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //We have to convert all the letters in the email body to lowercase. Otherwise, the String.replace() method may
        //loose different caption combinations of the same word(s).

        body = body.toLowerCase();
/*
        //First we have to find the three key people from the Enron email database and replace the three names with
        //representative names.
        int keyPeopleArrayLen = keyPeopleArray.length;

        for(int i=0; i<keyPeopleArrayLen; i++){
            body = body.replace(keyPeopleArray[i], replacementNamesArray[i]);
        }
*/
        return body;
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
