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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Extension(
        name = "filter",
        namespace = "emailProcessorBenchmark",
        description = "Conducts filtering of emails from the Enron email dataset. This function removes any email " +
                "which has from address from a different domain other than @enron.com.",
        parameters = {
                @Parameter(name = "arg1",
                        description = "A list of email addresses which need to be filtered.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the list of filtered email addresses as a comma separated list of email " +
                        "addresses.",
                type = {DataType.STRING}),
        examples = @Example(description = "If the input string has 'abc@enron.com, cde@wso2.com, efg@enron.com' " +
                "it will remove cde@wso2.com from the list and output 'abc@enron.com, efg@enron.com'",
                syntax = "filter(\"abc@enron.com, cde@wso2.com, efg@enron.com\")")
)

public class FilterFunction  extends FunctionExecutor {
    private String ENRON_DOMAIN = "enron.com";
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext
            siddhiAppContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
        boolean flag = false;
        List<CharSequence> addressesList = (List<CharSequence>)(objects[0]);
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

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    addressesListNew.add(item.toString());
                }
            }
        }

        return addressesListNew;
    }

    @Override
    protected Object execute(Object data) {
        String commaSeparatedAddresses = (String)data;
        Splitter splitter = Splitter.on(',');
        Iterator<String> dataStrIterator = splitter.split(commaSeparatedAddresses).iterator();
        String emailAddress = null;
        StringBuilder sb = new StringBuilder();

        while(dataStrIterator.hasNext()){
            emailAddress = dataStrIterator.next();
            if(emailAddress.contains(ENRON_DOMAIN)){
                sb.append(emailAddress);
            }
        }

        String result = sb.toString().trim();

        if(result.length() > 0) {
            return result;
        }else{
            return "";
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
