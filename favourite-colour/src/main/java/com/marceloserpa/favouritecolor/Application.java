package com.marceloserpa.favouritecolor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Application {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Set<String> allowedColors = Stream.of("red", "green", "blue").collect(Collectors.toSet());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textlines = builder.stream("favourite-colour-input");

        KStream<String, String> usersAndColours = textlines.filter((nullKey, line) -> line.contains(","))
            .selectKey((nullKey, line) -> line.split(",")[0].toLowerCase())
            .mapValues(line -> line.split(",")[1].toLowerCase())
            .filter((user, color) -> allowedColors.contains(color));

        usersAndColours.to("user-keys-colours");

        KTable<String, String> usersAndColorsTable = builder.table("user-keys-colours");

        KTable<String, Long> favouriteColours = usersAndColorsTable.groupBy((user, color) -> new KeyValue<>(color, color))
                .count("colorCounts");

        favouriteColours.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");


        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
