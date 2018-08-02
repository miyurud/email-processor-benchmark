package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;



import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;




public class KafkaMessageSender {

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";




    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put( ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        return new KafkaProducer<>(props);
    }



    public static void runProducer(String JSON) throws Exception {




        final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, JSON);




        try {
            producer.send(record,null);
            System.out.println("Message sent to kafka topic of "+TOPIC);

        }

        finally {
            producer.flush();
            producer.close();
        }
    }




}
