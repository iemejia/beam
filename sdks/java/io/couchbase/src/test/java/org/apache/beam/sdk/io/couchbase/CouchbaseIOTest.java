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
package org.apache.beam.sdk.io.couchbase;

import static org.apache.beam.sdk.io.couchbase.CouchbaseIO.ConnectionConfiguration;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.couchbase.CouchbaseContainer;

/** Test for {@link CouchbaseIO}. */
@RunWith(JUnit4.class)
public class CouchbaseIOTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static CouchbaseContainer couchbase;
  private static Bucket bucket;
  private static final int CARRIER_PORT = 11210;
  private static final int HTTP_PORT = 8091;
  private static final String BUCKET_NAME = "bucket-name";
  private static final String USERNAME = "admin";
  private static final String ADMIN_PASSWORD = "admin-pwd";
  private static final String BUCKET_PASSWORD = "bucket-pwd";
  private static final int SAMPLE_SIZE = 100;
  private static final String[] scientists = {
    "Einstein",
    "Darwin",
    "Copernicus",
    "Pasteur",
    "Curie",
    "Faraday",
    "Newton",
    "Bohr",
    "Galilei",
    "Maxwell"
  };

  @BeforeClass
  public static void startCouchbase() {
    couchbase =
        new CouchbaseContainer()
            .withClusterAdmin(USERNAME, ADMIN_PASSWORD)
            .withNewBucket(
                DefaultBucketSettings.builder()
                    .enableFlush(true)
                    .name(BUCKET_NAME)
                    .quota(100)
                    .password(BUCKET_PASSWORD)
                    .type(BucketType.COUCHBASE)
                    .build());
    couchbase.start();
    bucket = couchbase.getCouchbaseCluster().openBucket(BUCKET_NAME);
    insertData();
  }

  @AfterClass
  public static void stopCouchbase() {
    if (couchbase.isIndex() && couchbase.isQuery() && couchbase.isPrimaryIndex()) {
      bucket.query(
          N1qlQuery.simple(
              String.format("DELETE FROM `%s`", bucket.name()),
              N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)));
    } else {
      bucket.bucketManager().flush();
    }
  }

  @Test
  public void testRead() {
    ConnectionConfiguration connectionConfiguration =
        ConnectionConfiguration.builder()
            .setHosts(Collections.singletonList(couchbase.getContainerIpAddress()))
            .setHttpPort(couchbase.getMappedPort(HTTP_PORT))
            .setCarrierPort(couchbase.getMappedPort(CARRIER_PORT))
            .setBucket(BUCKET_NAME)
            .setPassword(BUCKET_PASSWORD)
            .build();
    PCollection<Document> output =
        pipeline.apply(
            CouchbaseIO.<JsonDocument>read()
                .withConnectionConfiguration(connectionConfiguration)
                .withCoder(SerializableCoder.of(JsonDocument.class)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo((long) SAMPLE_SIZE);
    PAssert.that(output)
        .satisfies(
            iterable -> {
              iterable.forEach(
                  doc ->
                      Assert.assertEquals(
                          JsonObject.create()
                              .put(
                                  "name",
                                  scientists[Integer.valueOf(doc.id()) % scientists.length]),
                          doc.content()));
              return null;
            });

    pipeline.run();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private static void insertData() {
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      bucket.upsert(
          JsonDocument.create(
              String.valueOf(i),
              JsonObject.create().put("name", scientists[i % scientists.length])));
    }
  }
}
