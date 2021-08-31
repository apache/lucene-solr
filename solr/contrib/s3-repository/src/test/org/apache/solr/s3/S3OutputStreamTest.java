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
import java.io.InputStream;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

public class S3OutputStreamTest extends SolrTestCaseJ4 {

  private static final String BUCKET = S3OutputStreamTest.class.getSimpleName();

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().silent().withInitialBuckets(BUCKET)
          .withProperty("spring.autoconfigure.exclude", "org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration")
          .withProperty("spring.jmx.enabled", "false")
          .withProperty("server.jetty.threads.idle-timeout", "3s")
          .build();

  private S3Client s3;

  @Before
  public void setUpClient() {
    s3 = S3_MOCK_RULE.createS3ClientV2();
  }

  @After
  public void tearDownClient() {
    s3.close();
  }

  /**
   * Basic check writing content byte-by-byte. They should be kept in the internal buffer and
   * flushed to S3 only once.
   */
  @Test
  public void testWriteByteByByte() throws IOException {
    try (S3OutputStream output = new S3OutputStream(s3, "byte-by-byte", BUCKET)) {
      output.write('h');
      output.write('e');
      output.write('l');
      output.write('l');
      output.write('o');
    }

    // Check we can re-read same content
    try (InputStream input = s3.getObject(b -> b.bucket(BUCKET).key("byte-by-byte"))) {
      String read = IOUtils.toString(input, Charset.defaultCharset());
      assertEquals("Contents saved to S3 file did not match expected", "hello", read);
    }
  }

  /** Write a small byte array, which is smaller than S3 part size. */
  @Test
  public void testWriteSmallBuffer() throws IOException {
    // must be smaller than S3 part size
    byte[] buffer = "hello".getBytes(Charset.defaultCharset());
    // pre-check -- ensure that our test string isn't too big
    assertTrue(buffer.length < S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "small-buffer", BUCKET)) {
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input = s3.getObject(b -> b.bucket(BUCKET).key("small-buffer"))) {
      String read = IOUtils.toString(input, Charset.defaultCharset());
      assertEquals("hello", read);
    }
  }

  /** Write a byte array larger than S3 part size. Simulate a real multi-part upload. */
  @Test
  public void testWriteLargeBuffer() throws IOException {
    // must be larger than S3 part size
    String content = RandomStringUtils.randomAlphanumeric(S3OutputStream.PART_SIZE + 1024);
    byte[] buffer = content.getBytes(Charset.defaultCharset());
    // pre-check -- ensure that our test string isn't too small
    assertTrue(buffer.length > S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "large-buffer", BUCKET)) {
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input = s3.getObject(b -> b.bucket(BUCKET).key("large-buffer"))) {
      String read = IOUtils.toString(input, Charset.defaultCharset());
      assertEquals(new String(buffer, Charset.defaultCharset()), read);
    }
  }

  /** Check flush is a no-op if data size is lower than required size of S3 part. */
  @Test
  public void testFlushSmallBuffer() throws IOException {
    // must be smaller than S3 minimal part size, so flush is a no-op
    byte[] buffer = "hello".getBytes(Charset.defaultCharset());
    assertTrue(buffer.length < S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-small", BUCKET)) {
      output.write(buffer);
      output.flush();

      buffer = ", world!".getBytes(Charset.defaultCharset());
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input = s3.getObject(b -> b.bucket(BUCKET).key("flush-small"))) {
      String read = IOUtils.toString(input, Charset.defaultCharset());
      assertEquals(
          "Flushing a small frame of an S3OutputStream should not impact data written",
          "hello, world!",
          read);
    }
  }

  /** Check flush is happening when data in buffer is larger than S3 minimal part size. */
  @Test
  public void testFlushLargeBuffer() throws IOException {
    // must be larger than S3 minimal part size, so we actually do something for flush()
    String content = RandomStringUtils.randomAlphanumeric(S3OutputStream.MIN_PART_SIZE + 1024);
    byte[] buffer = content.getBytes(Charset.defaultCharset());
    assertTrue(buffer.length > S3OutputStream.MIN_PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-large", BUCKET)) {
      output.write(buffer);
      output.flush();

      buffer = "some more".getBytes(Charset.defaultCharset());
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input = s3.getObject(b -> b.bucket(BUCKET).key("flush-large"))) {
      String read = IOUtils.toString(input, Charset.defaultCharset());
      assertEquals(
          "Flushing a large frame of an S3OutputStream should not impact data written",
          content + "some more",
          read);
    }
  }
}
