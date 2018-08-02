package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.joda.time.Instant;

import org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.performance.PerfStats;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by miyurud on 4/17/15.
 */
public class EmailProcessorSiddhi {
    private static String inputfilePath = "/home/cep/miyurud/data/enron.avro";
    private static PerfStats perfStats1 = new PerfStats();
    private static PerfStats perfStats2 = new PerfStats();
    private static long lastEventTime1 = -1;
    private static long lastEventTime2 = -1;
    private static long startTime;
    private static long differenceFromNTP = 0;
    private static int NITR = 10;
    private static long firstTupleTime = -1;
    private long timeOfFirstEventInjection = 0l;
    private String logDir = "/home/cep/miyurud/tmp";
    private FileWriter fw = null;
    private BufferedWriter bw = null;
    private StringBuilder stringBuilder = new StringBuilder();
    private static String COMMA = ",";
    private static String CARRIAGERETURN_NEWLINE = "\r\n";
    private static int PERFORMANCE_RECORDING_WINDOW = 10000; //This is the number of events to record.

    public static void main(String[] args){
        differenceFromNTP = getAverageTimeDifference(NITR);
        EmailProcessorSiddhi queryObj = new EmailProcessorSiddhi();
        queryObj.run();
    }

    private static org.joda.time.DateTime getNTPDate() {
        String[] hosts = new String[]{ "3.sg.pool.ntp.org", "0.jp.pool.ntp.org", "1.jp.pool.ntp.org"};

        NTPUDPClient client = new NTPUDPClient();
        // We want to timeout if a response takes longer than 5 seconds
        client.setDefaultTimeout(5000);

        for (String host : hosts) {

            try {
                InetAddress hostAddr = InetAddress.getByName(host);
                //System.out.println("> " + hostAddr.getHostName() + "/" + hostAddr.getHostAddress());
                TimeInfo info = client.getTime(hostAddr);
                org.joda.time.DateTime date = new org.joda.time.DateTime(info.getReturnTime());
                return date;

            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        client.close();

        return null;
    }

    /**
     * This is the average time that needs to be added to the local time to make it synchronized with the NTP time.
     * @param nitr
     * @return
     */
    private static long getAverageTimeDifference(int nitr) {
        long result = 0;
        long ntp = 0l;
        long local = 0l;

        for(int i = 0; i < nitr; i++) {
            ntp = getNTPDate().getMillis();
            local = new Instant().getMillis();

            result+=(ntp-local);
            //result += new Interval(getNTPDate().getMillis(), new Instant().getMillis()).toPeriod().getMillis();
        }

        return (result/nitr);
    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();
        //SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String,Class> extensions = new HashMap<String, Class>();
//        extensions.put("emailProcessorBenchmark:filter", FilterFunction.class);
//        extensions.put("emailProcessorBenchmark:modify", ModifyFunction.class);
//        extensions.put("emailProcessorBenchmark:mostFrequentWord", MostFrequentWordFunction.class);
//        extensions.p                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ut("emailProcessorBenchmark:metrics", MetricsFunction.class);
        //MostFrequentWordFunction

        //The following two functions should have been picked up from .siddhiext files, but this seems not functioning at the moment.s
//        extensions.put("regex:find", org.wso2.siddhi.extension.regex.FindFunctionExtension.class);
//        extensions.put("str:replace_all", org.wso2.siddhi.extension.string.ReplaceAllFunctionExtension.class);
        //siddhiContext.setSiddhiExtensions(extensions);

        String inputEmailStream = "define stream inputEmailsStream ( iij_timestamp long, fromAddress string, toAddresses string,"
                                  + "ccAddresses string, bccAddresses string, subject string, body string, regexstr string); ";

        //The following two queries (query 1 and query2) are used to filter the emails that do not originate from enron.com domain.
        String query1 = "@info(name = 'query1') from inputEmailsStream select iij_timestamp, regex:find(fromAddress, regexstr) as isValidFromAddress, fromAddress, toAddresses, ccAddresses, bccAddresses, subject, body insert into filteredEmailStream1;";

        String query2 = "@info(name = 'query2') from filteredEmailStream1[isValidFromAddress == true] select * insert into filteredEmailStream2;";

        String query3 = "@info(name = 'query3') from filteredEmailStream2 select iij_timestamp, fromAddress, emailProcessorBenchmark:filter(toAddresses) as toAdds, emailProcessorBenchmark:filter(ccAddresses) as ccAdds, emailProcessorBenchmark:filter(bccAddresses) as bccAdds, subject, body insert into filteredEmailStream3;";

        String query4 = "@info(name = 'query4') from filteredEmailStream3 select iij_timestamp, fromAddress, toAdds, ccAdds, bccAdds, subject, emailProcessorBenchmark:modify(body) as bodyObfuscated insert into modifiedEmailStream;";

        String query5 = "@info(name = 'query5') from modifiedEmailStream select iij_timestamp, fromAddress, toAdds, ccAdds, bccAdds, emailProcessorBenchmark:mostFrequentWord(bodyObfuscated, subject) as updatedSubject, bodyObfuscated insert into outputEmailStream;";

        String query6 = "@info(name = 'query6') from outputEmailStream select iij_timestamp, emailProcessorBenchmark:metrics(bodyObfuscated) as metrics insert into emailMetricsNonFilteredStream;";

        String query7 = "@info(name = 'query7') from emailMetricsNonFilteredStream select iij_timestamp, metrics output last every 10 sec insert into emailMetricsStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inputEmailStream+query1+query2+query3+query4+query5+query6+query7);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputEmailsStream");
        DataLoderThreadSiddhi dataLoderThreadSiddhi = new DataLoderThreadSiddhi(inputfilePath, inputHandler, differenceFromNTP);
        startTime = System.currentTimeMillis();
        dataLoderThreadSiddhi.start();

        executionPlanRuntime.addCallback("outputEmailStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event evt : events) {
                    long currentTime = System.currentTimeMillis() + differenceFromNTP;

                    perfStats1.count++;
                    perfStats1.lastEventTime = currentTime;

                    if (firstTupleTime == -1) {
                        firstTupleTime = currentTime;
                    }

                    long eventOriginationTime = Long.parseLong(evt.getData()[0].toString());
                    String body = evt.getData()[7].toString();
                    long latency = -1;
                    if (eventOriginationTime == -1l) {
                        //The following time difference is measured from the output side
                        long timeDifferenceFromStart = perfStats1.lastEventTime - firstTupleTime;
                        long timeDifferenceForEntireExecution = currentTime - timeOfFirstEventInjection;

                        System.out.println("timeDifferenceFromStart:"+timeDifferenceFromStart);
                        System.out.println("timeDifferenceForEntireExecution:"+timeDifferenceForEntireExecution);

                        try {
                            bw.write("throughput (events/second)" + COMMA + "Output data rate(events/second)" + COMMA + "Total elapsed time(s)" + COMMA + "average latency per event(s)" + CARRIAGERETURN_NEWLINE);
                            bw.write((Integer.parseInt(body) * 1000 / timeDifferenceForEntireExecution) + COMMA + (perfStats1.count * 1000 / timeDifferenceFromStart) + COMMA + (timeDifferenceForEntireExecution / 1000) + COMMA + (perfStats1.totalLatency / (perfStats1.count * 1000)) + CARRIAGERETURN_NEWLINE);
                            bw.flush();
                            bw.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            stringBuilder.setLength(0);
                        }
                    }else if (eventOriginationTime == -2l){
                        timeOfFirstEventInjection = Long.parseLong(body); //This time stamp is with the body of the email.
                    }else{
                        latency = currentTime - eventOriginationTime;
                        perfStats1.totalLatency += latency;
                    }
                }
            }
        });

        executionPlanRuntime.addCallback("emailMetricsStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
//                for (Event evt : events) {
//
//                }
            }
        });

        executionPlanRuntime.start();


        while (true) {
            try {
                if (lastEventTime1 == perfStats1.lastEventTime ) {
                    System.out.println();
                    System.out.println("***** Query 1 *****");
                    long timeDifferenceFromStart = perfStats1.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats1.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStart);
                    System.out.println("overall throughput (events/s) :" + ((perfStats1.count * 1000) / timeDifferenceFromStart));
                    System.out.println("overall avg latency (ms) :" + (perfStats1.totalLatency / perfStats1.count));
                    System.out.println();
                    break;
                } else {
                    lastEventTime1 = perfStats1.lastEventTime;
                    Thread.sleep(10*1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
