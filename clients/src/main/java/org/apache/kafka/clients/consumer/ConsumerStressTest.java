/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ConsumerStressTest {
  private static KafkaConsumer<String, String> makeConsumer() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return new KafkaConsumer<>(properties);
  }

  private static List<String> loadTopics(String topicsFilepath) throws Exception {
    List<String> topics = new ArrayList<>();
    BufferedReader bufferedReader = new BufferedReader(new FileReader(topicsFilepath));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      topics.add(line);
    }
    return topics;
  }

  public static void main(String[] args) throws Exception {
    int numConsumers = Integer.parseInt(args[0]);
    String topicsFilepath = args[1];
    final List<String> topics = loadTopics(topicsFilepath);
    System.out.println(String.format("consumers: %d topics filepath: %s", numConsumers, topicsFilepath));
    System.out.println(topics);
    ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
    for (int i = 0; i < numConsumers; i++) {
      final KafkaConsumer<String, String> consumer = makeConsumer();
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          consumer.subscribe(topics);
          while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(2000);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
              System.out.println(String.format("%s-%d %d: %s", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value()));
            }
          }
        }
      });
    }
  }
}
