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

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/** Basic test that write data and read them through the S3 client. */
public class S3ReadWriteTest extends AbstractS3ClientTest {

  /** Write and read a simple file (happy path). */
  @Test
  public void testBasicWriteRead() throws Exception {
    pushContent("/foo", "my blob");

    try (InputStream stream = client.pullStream("/foo")) {
      assertEquals("my blob", IOUtils.toString(stream, Charset.defaultCharset()));
    }
  }

  /** Check writing a file with no path. */
  @Test
  public void testWriteNoPath() {
    assertThrows(
        "Should not be able to write content to empty path",
        S3Exception.class,
        () -> pushContent("", "empty path"));
    assertThrows(
        "Should not be able to write content to root path",
        S3Exception.class,
        () -> pushContent("/", "empty path"));
  }

  /** Check reading a file with no path. */
  @Test
  public void testReadNoPath() {
    assertThrows(
        "Should not be able to read content from empty path",
        S3Exception.class,
        () -> client.pullStream(""));
    assertThrows(
        "Should not be able to read content from empty path",
        S3Exception.class,
        () -> client.pullStream("/"));
  }

  /** Test writing over an existing file and overriding the content. */
  @Test
  public void testWriteOverFile() throws Exception {
    pushContent("/override", "old content");
    pushContent("/override", "new content");

    InputStream stream = client.pullStream("/override");
    assertEquals(
        "File contents should have been overriden",
        "new content",
        IOUtils.toString(stream, Charset.defaultCharset()));
  }

  /** Check getting the length of a written file. */
  @Test
  public void testLength() throws Exception {
    pushContent("/foo", "0123456789");
    assertEquals(10, client.length("/foo"));
  }

  /** Check an exception is raised when getting the length of a directory. */
  @Test
  public void testDirectoryLength() throws Exception {
    client.createDirectory("/directory");

    S3Exception exception =
        assertThrows(
            "Getting length on a dir should throw exception",
            S3Exception.class,
            () -> client.length("/directory"));
    assertEquals("Path is Directory", exception.getMessage());
  }

  /** Check various method throws the expected exception of a missing S3 key. */
  @Test
  public void testNotFound() {
    assertThrows(S3NotFoundException.class, () -> client.pullStream("/not-found"));
    assertThrows(S3NotFoundException.class, () -> client.length("/not-found"));
    assertThrows(
        S3NotFoundException.class, () -> client.delete(Collections.singleton("/not-found")));
  }
}
