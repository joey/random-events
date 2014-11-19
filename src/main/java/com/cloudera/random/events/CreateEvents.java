/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.random.events;

import com.cloudera.random.event.StandardEvent;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateEvents {

  private static Logger LOG = LoggerFactory.getLogger(CreateEvents.class);

  protected Random random;
  protected long baseTimestamp;
  protected long counter;
  protected StandardEvent.Builder builder;
  protected Utf8 uuid;
  protected final Map<Utf8, Utf8> alert = ImmutableMap.of(
      new Utf8("type"), new Utf8("alert")
  );
  protected final Map<Utf8, Utf8> click =  ImmutableMap.of(
      new Utf8("type"), new Utf8("click")
  );

  private static Utf8 generateLargePayload() {
    StringBuilder buffer = new StringBuilder(1024);
    for (int i = 0; i < 1024; i++) {
      buffer.append(' ');
    }
    return  new Utf8(buffer.toString());
  }

  public CreateEvents() {
    random = new Random();
    baseTimestamp = System.currentTimeMillis();
    counter = 0l;
    builder = StandardEvent.newBuilder();
    uuid = new Utf8(UUID.randomUUID().toString());
  }

  public StandardEvent generateRandomEvent() {
    return builder
        .setEventInitiator(new Utf8("client_user"))
        .setEventName(randomEventName())
        .setUserId(randomUserId())
        .setSessionId(randomSessionId())
        .setIp(randomIp())
        .setTimestamp(randomTimestamp())
        .setEventDetails(randomEventDetails())
        .build();
  }

  public Utf8 randomEventName() {
    return new Utf8("event"+counter++);
  }

  public long randomUserId() {
    return random.nextInt(10);
  }

  public Utf8 randomSessionId() {
    return nextUuid();
  }

  public Utf8 nextUuid() {
    byte[] bytes = uuid.getBytes();
    int len = uuid.getByteLength();
    boolean carry = false;
    for (int idx = len -1; idx >= 0; idx--) {
      carry = false;
      byte b = (byte) (1 + bytes[idx]);
      if (b == '9') {
        b = 'a';
      } else if (b == 'g') {
        b = '0';
        carry = true;
      }

      bytes[idx] = b;

      if (!carry) {
        break;
      }
    }
    return uuid;
  }

  public Utf8 randomIp() {
    return new Utf8("192.168." + (random.nextInt(254) + 1) + "."
        + (random.nextInt(254) + 1));
  }

  public long randomTimestamp() {
    long delta = System.currentTimeMillis() - baseTimestamp;
    // Each millisecond elapsed will elapse 100 milliseconds
    // this is the equivalent of each second being 1.67 minutes
    delta = delta*100l;
    return baseTimestamp+delta;
  }

  public Map<Utf8, Utf8> randomEventDetails() {
    return random.nextInt(1500) < 1 ? alert : click;
  }

  private static String host;
  private static int port;
  private static RpcClient client;
  private static List<Event> batch;
  private static int batchSize = 0;
  private static int count = 0;
  private static Map<String, String> headers;

  public static void main(String[] args) throws IOException {
    CreateEvents createEvents = new CreateEvents();
    host = args[0];
    port = Integer.parseInt(args[1]);
    headers = ImmutableMap.of("flume.avro.schema.url", args[2]);

    client = RpcClientFactory.getDefaultInstance(host, port, 50000);
    batch = new LinkedList<>();

    long lastMsgTimestamp = System.currentTimeMillis();
    int lastBytes = 0;

    int bytes = 0;
    while (bytes < 1024*1024*1024) {
      StandardEvent event = createEvents.generateRandomEvent();
      int size = send(event);
      count++;
      bytes += size;
      batchSize += size;

      if (batchSize >= 5*1024*1024) {
        sendBatch();
      }

      if (System.currentTimeMillis() - lastMsgTimestamp >= TimeUnit.SECONDS.toMillis(60)) {
        lastMsgTimestamp = System.currentTimeMillis();
        LOG.info("Sent " + (bytes - lastBytes) + " bytes in 60 seconds");
        lastBytes = bytes;
      }
    }

    client.close();

    System.out.println("Sent " + bytes + " bytes");
    System.out.println("Sent " + count + " events");
  }

  private static BinaryEncoder encoder;

  private static int send(StandardEvent event) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
    DatumWriter<StandardEvent> writer = new SpecificDatumWriter<>(StandardEvent.class);
    encoder = EncoderFactory.get().binaryEncoder(out, encoder);
    writer.write(event, encoder);
    encoder.flush();

    batch.add(EventBuilder.withBody(out.toByteArray(), headers));
    
    return out.size();
  }

  private static void sendBatch() {
    try {
      client.appendBatch(batch);
    } catch (EventDeliveryException ex) {
      LOG.error("Delivery exception, dropping " + batch.size() + " events", ex);
      client.close();
      client = RpcClientFactory.getDefaultInstance(host, port);
    } finally {
      batch.clear();
      batchSize = 0;
    }
  }
}
