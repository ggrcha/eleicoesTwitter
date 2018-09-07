package com.github.ggrcha.eleicoesTwitter;

import com.google.common.collect.Lists;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "6kDugPxgxPoWciw1F0M3ws8oi";
    String consumerSecret = "ydQqtT03BiCCSe5SgeyuL62to82AxfOQBWzQaNYFmpLXuZVz1s";
    String token = "195128492-Hulv5pgcxIhAjrKTUid3W8Vw0ffwWcEyqkwc1Brh";
    String secret = "I6loMSpUzkvjKJX8fFPD0HjVmpiUCgCaqqfPSlfSdXTXK";


    public TwitterProducer(){};

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
//criar um cliente twitter

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        final Client client = criarClienteTwitter(msgQueue);

//criar um producer kafka

        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

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
                logger.info("msg: " + msg);
                kafkaProducer.send(new ProducerRecord<String, String>("eleicoes_twitter", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!= null) {
                            logger.error("erro: " + e.getMessage());
                        }
                    }
                });
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
        props.setProperty(ProducerConfig.ACKS_CONFIG,Integer.toString(1));

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
