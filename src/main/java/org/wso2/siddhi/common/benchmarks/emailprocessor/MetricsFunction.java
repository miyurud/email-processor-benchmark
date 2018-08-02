package org.wso2.siddhi.common.benchmarks.emailprocessor;

import com.google.common.base.Splitter;
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

import java.util.Iterator;
import java.util.Map;


@Extension(
        name = "metrics",
        namespace = "emailProcessorBenchmark",
        description = "Calculated the metrics of the emails.",
        parameters = {
                @Parameter(name = "arg1",
                        description = "The body of the email to for which the metrics to be calculated.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns a comma separated list of three numbers indicating the count of characters" +
                        " words and paragraphs.",
                type = {DataType.STRING}),
        examples = @Example(description = "If the input was 'The meeting was held.', the output should be 21,4,0",
                syntax = "metrics(\"Email body\")")
)
public class MetricsFunction  extends FunctionExecutor {
    private long emailCounter;
    private long windowDuration = 10000;//time is in ms
    private long windowStartTime;
    private String COMMA = ",";

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
        return null;
    }

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
