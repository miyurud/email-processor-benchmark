package org.wso2.siddhi.common.benchmarks.emailprocessor;

import javax.mail.internet.MimeUtility;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

@Extension(
        name = "modify",
        namespace = "emailProcessorBenchmark",
        description = "Conducts modification of the email body by observing the appearance of the name of the three " +
                "people (Kenneth Lay, Jeffrey Skilling, and Andrew Fastow).",
        parameters = {
                @Parameter(name = "arg1",
                        description = "The body of the email to be modified if the names of the three persons appear.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the email body with obfuscated person names",
                type = {DataType.STRING}),
        examples = @Example(description = "If the body had 'Hello Mr. Kenneth Lay.', it will get changed to 'Hello " +
                "Mr. Person1'",
                syntax = "modify(\"Email body\")")
)
public class ModifyFunction extends FunctionExecutor  {
    private String[] keyPeopleArray = new String[]{"Kenneth Lay","Jeffrey Skilling","Andrew Fastow"};
    private String[] replacementNamesArray = new String[]{"Person1","Person2","Person3"};

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
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

        //First we have to find the three key people from the Enron email database and replace the three names with
        //representative names.
        int keyPeopleArrayLen = keyPeopleArray.length;

        for(int i=0; i<keyPeopleArrayLen; i++){
            body = body.replace(keyPeopleArray[i], replacementNamesArray[i]);
        }

        return body;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
