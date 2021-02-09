package me.jko.demoreplyingkafkatemplateclient;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public ApplicationRunner runner(ReplyingKafkaTemplate<String, String, String> template) {
    return args -> {
      ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", "foo");

      // default timeout is 5 seconds, so if you want to wait more, add duration parameter
      RequestReplyFuture<String, String, String> replyFuture = template.sendAndReceive(record, Duration.ofSeconds(10));
      SendResult<String, String> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);
      System.out.println("Sent ok: " + sendResult.getRecordMetadata());
      ConsumerRecord<String, String> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
      System.out.println("Return value: " + consumerRecord.value());
    };
  }

  @Bean
  public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
      ProducerFactory<String, String> pf,
      ConcurrentMessageListenerContainer<String, String> replyContainer) {

    return new ReplyingKafkaTemplate<>(pf, replyContainer);
  }

  @Bean
  public ConcurrentMessageListenerContainer<String, String> repliesContainer(
      ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {

    ConcurrentMessageListenerContainer<String, String> repliesContainer =
        containerFactory.createContainer("replies");
    repliesContainer.getContainerProperties().setGroupId("repliesGroup");
    repliesContainer.setAutoStartup(false);

    return repliesContainer;
  }

  // When you want to use shared topic
  @Bean
  public ConcurrentMessageListenerContainer<String, String> replyContainer(
      ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {

    ConcurrentMessageListenerContainer<String, String> container = containerFactory.createContainer("topic2");
    container.getContainerProperties().setGroupId(UUID.randomUUID().toString()); // unique
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // so the new group doesn't get old replies
    container.getContainerProperties().setKafkaConsumerProperties(props);

    return container;
  }


  @Bean
  public NewTopic kRequests() {
    return TopicBuilder.name("kRequests")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Bean
  public NewTopic kReplies() {
    return TopicBuilder.name("kReplies")
        .partitions(3)
        .replicas(2)
        .build();
  }
}
