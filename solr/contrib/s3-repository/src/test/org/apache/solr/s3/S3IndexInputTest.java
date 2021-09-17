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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test for reading files from S3 'the Solr way'. */
public class S3IndexInputTest extends SolrTestCaseJ4 {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /** Simulates fetching a file from S3 in more than one read operation. */
  @Test
  public void testPartialReadSmallSlice() throws IOException {
    String content = "This is much too large for a single slice";
    int slice = 5;

    doTestPartialRead(false, content, slice);
    doTestPartialRead(true, content, slice);
  }

  /**
   * Simulates fetching a file from S3 in more than one read operation, reading larger than internal
   * buffer size.
   */
  @Test
  public void testPartialReadBigSlice() throws IOException {
    // Large text, be sure we can't read in a single I/O call
    int length = S3IndexInput.LOCAL_BUFFER_SIZE * 10 + 5;
    String content = RandomStringUtils.randomAscii(length);

    int slice = S3IndexInput.LOCAL_BUFFER_SIZE * 2;
    doTestPartialRead(false, content, slice);
    doTestPartialRead(true, content, slice);
  }

  private void doTestPartialRead(boolean directBuffer, String content, int slice)
      throws IOException {

    File tmp = temporaryFolder.newFolder();
    File file = new File(tmp, "content");
    FileUtils.write(file, content, StandardCharsets.UTF_8);

    try (SliceInputStream slicedStream = new SliceInputStream(new FileInputStream(file), slice);
         S3IndexInput input = new S3IndexInput(slicedStream, "path", file.length())) {

      // Now read the file
      ByteBuffer buffer;
      if (directBuffer) {
        buffer = ByteBuffer.allocateDirect((int) file.length());
      } else {
        buffer = ByteBuffer.allocate((int) file.length());
      }
      input.readInternal(buffer);

      // Check the buffer content, in a way that works for both heap and direct buffers
      buffer.position(0);
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      assertEquals(content, new String(bytes, Charset.defaultCharset()));

      // Ensure we actually made many calls
      int expectedReadCount;
      if (directBuffer) {
        // For direct buffer, we may be caped by internal buffer if it's smaller than the size
        // defined by the test
        expectedReadCount = content.length() / Math.min(slice, S3IndexInput.LOCAL_BUFFER_SIZE) + 1;
      } else {
        expectedReadCount = content.length() / slice + 1;
      }
      assertEquals(
          "S3IndexInput did an unexpected number of reads",
          expectedReadCount,
          slicedStream.readCount);
    }
  }

  /** Input stream that reads, but not too much in a single call. */
  private static class SliceInputStream extends InputStream {

    private final InputStream input;
    private final int slice;

    // for testing, number of calls to read() method.
    private int readCount;

    SliceInputStream(InputStream input, int slice) {
      this.input = input;
      this.slice = slice;
    }

    @Override
    public int read() throws IOException {
      return input.read();
    }

    @Override
    public int read(byte[] b, int off, int length) throws IOException {
      readCount++;
      int slicedLength = Math.min(slice, length);
      return super.read(b, off, slicedLength);
    }

    @Override
    public void close() throws IOException {
      input.close();
      super.close();
    }
  }
}
