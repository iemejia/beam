/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.mqtt;

import static org.apache.beam.sdk.io.mqtt.MqttIO.MqttCheckpointMark;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.SerializationUtils;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link MqttIO}. */
@RunWith(JUnit4.class)
public class MqttIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(MqttIOTest.class);

  private static BrokerService server;
  private static int port;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting ActiveMQ server on {}", port);
    server = new BrokerService();
    server.setDeleteAllMessagesOnStartup(true);
    // use memory persistence for the embedded server
    server.setPersistent(false);
    server.addConnector("mqtt://localhost:" + port);
    server.start();
    server.waitUntilStarted();
  }

  private static List<byte[]> buildData(int size) {
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      data.add(("Test" + i).getBytes(StandardCharsets.UTF_8));
    }
    return data;
  }

  /**
   * @return A thread that produce messages to the broker service. This thread prevents to block the
   *     pipeline waiting for new messages
   */
  private static Thread buildPublisherThread(String host, String topicName, List<byte[]> messages) {
    return new Thread(
        () -> {
          try {
            MQTT client = new MQTT();
            client.setHost(host);
            client.setClientId("buildPublisherThread");
            client.setCleanSession(true);
            final BlockingConnection publishConnection = client.blockingConnection();
            publishConnection.connect();

            LOG.info("Waiting pipeline connected to the MQTT broker before sending messages ...");
            boolean pipelineConnected = false;
            while (!pipelineConnected) {
              Thread.sleep(1000);
              for (Connection connection : server.getBroker().getClients()) {
                if (!connection.getConnectionId().isEmpty()) {
                  pipelineConnected = true;
                }
              }
            }

            for (byte[] message : messages) {
              publishConnection.publish(topicName, message, QoS.EXACTLY_ONCE, false);
            }

            publishConnection.disconnect();
          } catch (Exception e) {
            // nothing to do
            LOG.error("Something went wrong while publishing MQTTIOTest events", e);
          }
        });
  }

  @Test
  public void testRead() throws Exception {
    final String topicName = "READ_TOPIC";
    int numElements = 19;
    final List<byte[]> data = buildData(numElements);

    PCollection<byte[]> output =
        p.apply(
            MqttIO.read()
                .withConnectionConfiguration(
                    MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, topicName)
                        .withClientId("READ_PIPELINE"))
                .withMaxReadTime(Duration.standardSeconds(3)));
    PAssert.that(output).containsInAnyOrder(data);

    Thread publisherThread = buildPublisherThread("tcp://localhost:" + port, topicName, data);
    publisherThread.start();
    p.run();
    publisherThread.join();
  }

  @Test
  public void testWrite() throws Exception {
    final int numElements = 10;

    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    client.setClientId("testWrite");
    client.setCleanSession(true);
    final BlockingConnection connection = client.blockingConnection();
    connection.connect();
    connection.subscribe(new Topic[] {new Topic(Buffer.utf8("WRITE_TOPIC"), QoS.EXACTLY_ONCE)});

    final Set<String> messages = new ConcurrentSkipListSet<>();
    Thread subscriber =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < numElements; i++) {
                  Message message = connection.receive();
                  messages.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                  message.ack();
                }
              } catch (Exception e) {
                LOG.error("Can't receive message", e);
              }
            });

    subscriber.start();
    List<byte[]> data = buildData(numElements);
    p.apply(Create.of(data))
        .apply(
            MqttIO.write()
                .withConnectionConfiguration(
                    MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, "WRITE_TOPIC"))
                .withRetained(false));
    p.run();
    subscriber.join();

    connection.disconnect();
    assertEquals(numElements, messages.size());
    for (int i = 0; i < numElements; i++) {
      assertTrue(messages.contains("Test " + i));
    }
  }

  @Test
  public void testMqttCheckpointMarkSerialization() {
    MqttCheckpointMark cp1 = new MqttCheckpointMark(UUID.randomUUID().toString());
    MqttCheckpointMark cp2 = SerializationUtils.deserialize(SerializationUtils.serialize(cp1));
    assertEquals(cp1, cp2);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (server != null) {
      server.stop();
      server.waitUntilStopped();
      server = null;
    }
  }
}
