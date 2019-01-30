package cn.itweet.sendmsg;

/**
 * Created by whoami on 2016/11/24.
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class SendMessageKafka {

    private static String topic = "order";
    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.110.181.41:6667");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "1");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int id = r.nextInt(10000000);
            int memberid = r.nextInt(100000);
            int totalprice = r.nextInt(1000) + 100;
            int preferential = r.nextInt(100);
            int sendpay = r.nextInt(3);

            StringBuffer data = new StringBuffer();
            data.append(String.valueOf(id)).append("\t")
                    .append(String.valueOf(memberid)).append("\t")
                    .append(String.valueOf(totalprice)).append("\t")
                    .append(String.valueOf(preferential)).append("\t")
                    .append(String.valueOf(sendpay)).append("\t")
                    .append(df.format(new Date()));
            System.out.println(data.toString());
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, data.toString());
            // send synchronously
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("send over ------------------");
    }

}
