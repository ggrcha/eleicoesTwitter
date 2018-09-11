package com.github.ggrcha.kafkaStreamFilter;

import com.google.gson.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFilterTweets {

    final static Logger logger = LoggerFactory.getLogger(StreamFilterTweets.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

//        criar propriedades

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"filtroTweets");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

//        criar topologia

        StreamsBuilder sb = new StreamsBuilder();

        KStream<String,String> inputTopic = sb.stream("eleicoes_twitter");
        KStream<String,String> filteredTopic = inputTopic.filter(
                (k,jsonTweets) ->  extractValueFromJason("lula",jsonTweets)
        );

        filteredTopic.to("eleicoes_lula");
//        construir a topologia

        KafkaStreams kafkaStreams = new KafkaStreams(
                sb.build(),
                props
        );

//        iniciar a aplicação stream
        kafkaStreams.start();

    }

    private static boolean extractValueFromJason(String value, String json) {

        return jsonParser.parse(json).getAsJsonObject().get("text").getAsString().contains(value);

    }
}
