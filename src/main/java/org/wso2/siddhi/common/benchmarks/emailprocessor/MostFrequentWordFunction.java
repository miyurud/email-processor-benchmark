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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Extension(
        name = "mostFrequentWord",
        namespace = "emailProcessorBenchmark",
        description = "Calculate the most frequent word in the body of the email.",
        parameters = {
                @Parameter(name = "arg1",
                        description = "The body of the email.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns a new subject for the email which is of the format " +
                        "'subject:<most frequent word>'. This subject will be propagated downstream.",
                type = {DataType.STRING}),
        examples = @Example(description = "If the input string has 'The performance of the CPU.', the most frequent " +
                "word becomes 'the'.",
                syntax = "mostFrequentWord(\"The performance of the CPU.\")")
)

public class MostFrequentWordFunction extends FunctionExecutor {
    private final String NONE = "NONE";
    private final String COLON = ":";

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] data) {
        String body = data[0].toString();
        String subject = data[1].toString();

        String mostFrequentWord = getMostFrequentWord(body);
        subject = subject + COLON + mostFrequentWord;
        return subject;
    }

    @Override
    protected Object execute(Object o) {
        return null;
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
