package com.motaz.topology;

import com.motaz.dto.AverageSpending;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;
import io.confluent.connect.avro.Transactions;

@Slf4j
@Component
public class AverageTransactionTopology {

    @Value("${spring.kafka.topics.transaction_topic}")
    private String TRANSACTION_TOPIC;

    @Value("${spring.kafka.topics.average-spending_topic}")
    private String TRANSACTION_AVERAGE_SPENDING_TOPIC;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String SCHEMA_REGISTRY_URL;

    // Source stream from the topic
    @Autowired
    public void process(StreamsBuilder streamsBuilder){
        //create serde for Serialization and Deserialization
        Serde<String> stringSerde = Serdes.String();
        Serde<AverageSpending> averageSpendingSerde = new JsonSerde<>(AverageSpending.class);
        Serde<Transactions> transactionSerde = createTransactionAvroSerde(SCHEMA_REGISTRY_URL);

        KStream<String,Transactions> transactionStream = streamsBuilder.stream(TRANSACTION_TOPIC,
                Consumed.with(stringSerde,transactionSerde)
        );

        transactionStream
                .print(Printed.<String,Transactions>toSysOut().withLabel("transactions"));

        // Group by customerId, then aggregate
        KTable<String, AverageSpending> averageSpendingTable = transactionStream
                .filter((key,transaction)->isCreditTransaction(transaction))
                .groupBy((key, transaction) -> transaction.getCustomerId(), Grouped.with(stringSerde, transactionSerde))
                .aggregate(
                        AverageSpending::new,
                        (customerId, transaction, agg) -> updateAverageSpending(customerId, transaction, agg),
                        Materialized.with(stringSerde, averageSpendingSerde)
                );

        //Convert to Stream
        KStream<String, AverageSpending> averageSpendingStream =
                averageSpendingTable.toStream();

        //print stream
        averageSpendingStream
                .print(Printed.<String, AverageSpending>toSysOut().withLabel("averageSpending"));

        // Write the aggregation results back to a new topic
        averageSpendingStream.to(TRANSACTION_AVERAGE_SPENDING_TOPIC,
                Produced.with(stringSerde, averageSpendingSerde));

        // Build and print the topology
        Topology topology = streamsBuilder.build();
        log.info("Kafka Streams Topology:\n{}", topology.describe());
    }

    private Serde<Transactions> createTransactionAvroSerde(String schemaRegistryUrl) {
        Serde<Transactions> serde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        serde.configure(config, false); // 'false' for value (true for key)
        return serde;
    }

    private boolean isCreditTransaction(Transactions transaction) {
        return transaction != null
                && transaction.getTransactionAmount() != null
                && transaction.getCustomerId() != null
                &&"Credit".equalsIgnoreCase(transaction.getTransactionType());
    }

    private AverageSpending updateAverageSpending(String customerId, Transactions transaction, AverageSpending averageSpending) {
        averageSpending.setCustomerId(customerId);
        averageSpending.setTotalAmount(averageSpending.getTotalAmount() + Double.valueOf(transaction.getTransactionAmount()));
        averageSpending.setTransactionCount(averageSpending.getTransactionCount() + 1);
        return averageSpending;
    }
}
