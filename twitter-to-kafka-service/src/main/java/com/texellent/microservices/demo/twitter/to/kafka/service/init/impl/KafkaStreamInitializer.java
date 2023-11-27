package com.texellent.microservices.demo.twitter.to.kafka.service.init.impl;

import com.texellent.microservices.demo.config.KafkaConfigData;
import com.texellent.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.texellent.microservices.demo.twitter.to.kafka.service.init.StreamInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger LOG= LoggerFactory.getLogger(KafkaStreamInitializer.class);
    private final KafkaConfigData kafkaConfigData;
    private final KafkaAdminClient kafkadminClient;

    public KafkaStreamInitializer(KafkaConfigData configData, KafkaAdminClient adminClient) {
        this.kafkaConfigData = configData;
        this.kafkadminClient = adminClient;
    }

    @Override
    public void init() {
        kafkadminClient.createTopics();
        kafkadminClient.checkSchemaRegistry();
        LOG.info("Topics with name {} are ready for operations", kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
