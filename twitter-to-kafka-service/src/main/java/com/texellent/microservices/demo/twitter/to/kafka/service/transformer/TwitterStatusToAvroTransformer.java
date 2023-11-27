package com.texellent.microservices.demo.twitter.to.kafka.service.transformer;

import com.texellent.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class TwitterStatusToAvroTransformer {

    public TwitterAvroModel getTwitterAveroModelFromStatus(Status status){
        return TwitterAvroModel
                .newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt().getTime())
                .build();
            }
}
