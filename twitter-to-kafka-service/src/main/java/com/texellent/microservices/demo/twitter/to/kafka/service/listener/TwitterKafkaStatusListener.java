package com.texellent.microservices.demo.twitter.to.kafka.service.listener;

import com.texellent.microservices.demo.kafka.producer.config.service.KafkaProducer;
import com.texellent.microservices.demo.config.KafkaConfigData;
import com.texellent.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.texellent.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import org.slf4j.LoggerFactory;
import  org.slf4j.Logger;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
private static final Logger LOG=LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

private final KafkaConfigData kafkaConfigData;
private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

private final TwitterStatusToAvroTransformer twitterKafkaStatusToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData configData, KafkaProducer<Long, TwitterAvroModel> producer, TwitterStatusToAvroTransformer transformer) {
        this.kafkaConfigData = configData;
        this.kafkaProducer = producer;
        this.twitterKafkaStatusToAvroTransformer = transformer;
    }


    @Override
    public void onStatus(Status status) {
    LOG.info("Received  status text  {} sending kafka topic {} ",status.getText(),kafkaConfigData.getTopicName());
    TwitterAvroModel twitterAvroModel=twitterKafkaStatusToAvroTransformer.getTwitterAveroModelFromStatus((status));
    kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
