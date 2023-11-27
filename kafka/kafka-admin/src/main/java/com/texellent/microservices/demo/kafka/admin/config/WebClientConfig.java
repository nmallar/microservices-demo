package com.texellent.microservices.demo.kafka.admin.config;

import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientSsl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {
   @Bean
   WebClient webClient(){
      return WebClient.builder().build();
   }
}
