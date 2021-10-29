/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.s3;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

/** Abstract class for test with S3Mock. */
public class AbstractS3ClientTest extends SolrTestCaseJ4 {

  private static final String BUCKET_NAME = "test-bucket";

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().silent().withInitialBuckets(BUCKET_NAME)
          .withProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration")
          .withProperty("spring.jmx.enabled", "false")
          .withProperty("server.jetty.threads.idle-timeout", "3s")
          .build();

  S3StorageClient client;

  @Before
  public void setUpClient() {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    client =
        new S3StorageClient(
            BUCKET_NAME,
            null,
            "us-east-1",
            "",
            false,
            "http://localhost:" + S3_MOCK_RULE.getHttpPort(),
            false);
  }

  @After
  public void tearDownClient() {
    client.close();
  }

  /**
   * Helper method to push a string to S3.
   *
   * @param path Destination path in S3.
   * @param content Arbitrary content for the test.
   */
  void pushContent(String path, String content) throws S3Exception {
    try (OutputStream output = client.pushStream(path)) {
      IOUtils.write(content, output, Charset.defaultCharset());
    } catch (IOException e) {
      throw new S3Exception(e);
    }
  }
}
