package com.github.ggrcha.eleicoesTwitter;

import com.github.ggrcha.eleicoesTwitter.metricas.MetricsProducerReporter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.xml.internal.bind.v2.TODO;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TwitterProducer implements Runnable{

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "6kDugPxgxPoWciw1F0M3ws8oi";
    String consumerSecret = "ydQqtT03BiCCSe5SgeyuL62to82AxfOQBWzQaNYFmpLXuZVz1s";
    String token = "195128492-Hulv5pgcxIhAjrKTUid3W8Vw0ffwWcEyqkwc1Brh";
    String secret = "I6loMSpUzkvjKJX8fFPD0HjVmpiUCgCaqqfPSlfSdXTXK";
    String topico = "eleicoes_twitter";


    public TwitterProducer(){};

    public static void main(String[] args) {

        new TwitterProducer().run();
    }


    static class MetricPair {
        private final MetricName metricName;
        private final Metric metric;
        MetricPair(MetricName metricName, Metric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }
        public String toString() {
            return metricName.group() + "." + metricName.name();
        }
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {

        final Set<String> metricsNameFilter = Sets.newHashSet(
                "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
                "request-size-max", "network-io-rate", "record-queue-time-avg",
                "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight"
        );

        final Map<String, TwitterProducer.MetricPair> metricsDisplayMap = metrics.entrySet().stream()
                //Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry ->
                        metricsNameFilter.contains(metricNameEntry.getKey().name()))
                //Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry ->
                        !Double.isInfinite(metricNameEntry.getValue().value()) &&
                                !Double.isNaN(metricNameEntry.getValue().value()) &&
                                metricNameEntry.getValue().value() != 0
                )
                //Turn Map<MetricName,Metric> into TreeMap<String, MetricPair>
                .map(entry -> new TwitterProducer.MetricPair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(
                        TwitterProducer.MetricPair::toString, it -> it, (a, b) -> a, TreeMap::new
                ));


        //display das metricas
        final StringBuilder builder = new StringBuilder(255);
        builder.append("\n---------------------------------------\n");
        metricsDisplayMap.entrySet().forEach(entry -> {
            TwitterProducer.MetricPair metricPair = entry.getValue();
            String name = entry.getKey();
            builder.append(String.format(Locale.US, "%50s%25s\t\t%,-10.2f\t\t%s\n",
                    name,
                    metricPair.metricName.name(),
                    metricPair.metric.value(),
                    metricPair.metricName.description()
            ));
        });
        builder.append("\n---------------------------------------\n");
        logger.info(builder.toString());
    }


    public void run() {
//criar um cliente twitter

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        final Client client = criarClienteTwitter(msgQueue);

//criar um producer kafka

        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();
        final Map<MetricName, ? extends Metric> metrics = kafkaProducer.metrics();

// Attempts to establish a connection.
        client.connect();

//adiciona shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("entrou shutdown hook");
                client.stop();
                kafkaProducer.close();
                logger.info("aplicação encerrada");
            }
        ));

        int i=0;

//enviar tweets para o kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!= null) {
//                logger.info("msg: " + msg);
                kafkaProducer.send(new ProducerRecord<String, String>(topico, null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!= null) {
                            logger.error("erro: " + e.getMessage());
                        }
                    }
                });
                i++;
                if(i==50) {
                    displayMetrics(metrics);
                    i=0;
                }

            }

        }
        logger.info("aplicação encerrada");
    }

    public KafkaProducer<String,String> createKafkaProducer(){
        //        propriedades do producer
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.17.152.79:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

//        compressão dos dados
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

//        configuração de batchs
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG,Integer.toString(20));
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32 kb - testar

//        criando um "safe producer".
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));

        return new KafkaProducer<String, String>(props);
    }

    public Client criarClienteTwitter(BlockingQueue<String> msgQueue){

        logger.info("preparando cliente ...");

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bolsonaro", "ciro","haddad","marina","alckmin");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,
                consumerSecret,
                token,
                secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
