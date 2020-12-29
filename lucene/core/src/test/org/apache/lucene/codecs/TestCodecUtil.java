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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;

/** tests for codecutil methods */
public class TestCodecUtil extends LuceneTestCase {

  public void testHeaderLength() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");
    input.seek(CodecUtil.headerLength("FooBar"));
    assertEquals("this is the data", input.readString());
    input.close();
  }

  public void testWriteTooLongHeader() throws Exception {
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      tooLong.append('a');
    }
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CodecUtil.writeHeader(output, tooLong.toString(), 5);
        });
  }

  public void testWriteNonAsciiHeader() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CodecUtil.writeHeader(output, "\u1234", 5);
        });
  }

  public void testReadHeaderWrongMagic() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    output.writeInt(1234);
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");
    expectThrows(
        CorruptIndexException.class,
        () -> {
          CodecUtil.checkHeader(input, "bogus", 1, 1);
        });
  }

  public void testChecksumEntireFile() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");
    CodecUtil.checksumEntireFile(input);
    input.close();
  }

  public void testCheckFooterValid() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();

    ChecksumIndexInput input =
        new BufferedChecksumIndexInput(new ByteBuffersIndexInput(out.toDataInput(), "temp"));
    Exception mine = new RuntimeException("fake exception");
    RuntimeException expected =
        expectThrows(
            RuntimeException.class,
            () -> {
              CodecUtil.checkFooter(input, mine);
            });
    assertEquals("fake exception", expected.getMessage());
    Throwable suppressed[] = expected.getSuppressed();
    assertEquals(1, suppressed.length);
    assertTrue(suppressed[0].getMessage().contains("checksum passed"));
    input.close();
  }

  public void testCheckFooterValidAtFooter() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();

    ChecksumIndexInput input =
        new BufferedChecksumIndexInput(new ByteBuffersIndexInput(out.toDataInput(), "temp"));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    Exception mine = new RuntimeException("fake exception");
    RuntimeException expected =
        expectThrows(
            RuntimeException.class,
            () -> {
              CodecUtil.checkFooter(input, mine);
            });
    assertEquals("fake exception", expected.getMessage());
    Throwable suppressed[] = expected.getSuppressed();
    assertEquals(1, suppressed.length);
    assertTrue(suppressed[0].getMessage().contains("checksum passed"));
    input.close();
  }

  public void testCheckFooterValidPastFooter() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();

    ChecksumIndexInput input =
        new BufferedChecksumIndexInput(new ByteBuffersIndexInput(out.toDataInput(), "temp"));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    // bogusly read a byte too far (can happen)
    input.readByte();
    Exception mine = new RuntimeException("fake exception");
    CorruptIndexException expected =
        expectThrows(
            CorruptIndexException.class,
            () -> {
              CodecUtil.checkFooter(input, mine);
            });
    assertTrue(expected.getMessage().contains("checksum status indeterminate"));
    Throwable suppressed[] = expected.getSuppressed();
    assertEquals(1, suppressed.length);
    assertEquals("fake exception", suppressed[0].getMessage());
    input.close();
  }

  public void testCheckFooterInvalid() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    output.writeInt(CodecUtil.FOOTER_MAGIC);
    output.writeInt(0);
    output.writeLong(1234567); // write a bogus checksum
    output.close();

    ChecksumIndexInput input =
        new BufferedChecksumIndexInput(new ByteBuffersIndexInput(out.toDataInput(), "temp"));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    Exception mine = new RuntimeException("fake exception");
    CorruptIndexException expected =
        expectThrows(
            CorruptIndexException.class,
            () -> {
              CodecUtil.checkFooter(input, mine);
            });
    assertTrue(expected.getMessage().contains("checksum failed"));
    Throwable suppressed[] = expected.getSuppressed();
    assertEquals(1, suppressed.length);
    assertEquals("fake exception", suppressed[0].getMessage());
    input.close();
  }

  public void testSegmentHeaderLength() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    CodecUtil.writeIndexHeader(output, "FooBar", 5, StringHelper.randomId(), "xyz");
    output.writeString("this is the data");
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");
    input.seek(CodecUtil.indexHeaderLength("FooBar", "xyz"));
    assertEquals("this is the data", input.readString());
    input.close();
  }

  public void testWriteTooLongSuffix() throws Exception {
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 256; i++) {
      tooLong.append('a');
    }
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CodecUtil.writeIndexHeader(
              output, "foobar", 5, StringHelper.randomId(), tooLong.toString());
        });
  }

  public void testWriteVeryLongSuffix() throws Exception {
    StringBuilder justLongEnough = new StringBuilder();
    for (int i = 0; i < 255; i++) {
      justLongEnough.append('a');
    }
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    byte[] id = StringHelper.randomId();
    CodecUtil.writeIndexHeader(output, "foobar", 5, id, justLongEnough.toString());
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");
    CodecUtil.checkIndexHeader(input, "foobar", 5, 5, id, justLongEnough.toString());
    assertEquals(input.getFilePointer(), input.length());
    assertEquals(
        input.getFilePointer(), CodecUtil.indexHeaderLength("foobar", justLongEnough.toString()));
    input.close();
  }

  public void testWriteNonAsciiSuffix() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          CodecUtil.writeIndexHeader(output, "foobar", 5, StringHelper.randomId(), "\u1234");
        });
  }

  public void testReadBogusCRC() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    output.writeLong(-1L); // bad
    output.writeLong(1L << 32); // bad
    output.writeLong(-(1L << 32)); // bad
    output.writeLong((1L << 32) - 1); // ok
    output.close();
    IndexInput input =
        new BufferedChecksumIndexInput(new ByteBuffersIndexInput(out.toDataInput(), "temp"));
    // read 3 bogus values
    for (int i = 0; i < 3; i++) {
      expectThrows(
          CorruptIndexException.class,
          () -> {
            CodecUtil.readCRC(input);
          });
    }
    // good value
    CodecUtil.readCRC(input);
  }

  public void testWriteBogusCRC() throws Exception {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    AtomicLong fakeChecksum = new AtomicLong();

    // wrap the index input where we control the checksum for mocking
    IndexOutput fakeOutput =
        new IndexOutput("fake", "fake") {
          @Override
          public void close() throws IOException {
            output.close();
          }

          @Override
          public long getFilePointer() {
            return output.getFilePointer();
          }

          @Override
          public long getChecksum() throws IOException {
            return fakeChecksum.get();
          }

          @Override
          public void writeByte(byte b) throws IOException {
            output.writeByte(b);
          }

          @Override
          public void writeBytes(byte[] b, int offset, int length) throws IOException {
            output.writeBytes(b, offset, length);
          }
        };

    fakeChecksum.set(-1L); // bad
    expectThrows(
        IllegalStateException.class,
        () -> {
          CodecUtil.writeCRC(fakeOutput);
        });

    fakeChecksum.set(1L << 32); // bad
    expectThrows(
        IllegalStateException.class,
        () -> {
          CodecUtil.writeCRC(fakeOutput);
        });

    fakeChecksum.set(-(1L << 32)); // bad
    expectThrows(
        IllegalStateException.class,
        () -> {
          CodecUtil.writeCRC(fakeOutput);
        });

    fakeChecksum.set((1L << 32) - 1); // ok
    CodecUtil.writeCRC(fakeOutput);
  }

  public void testTruncatedFileThrowsCorruptIndexException() throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    IndexOutput output = new ByteBuffersIndexOutput(out, "temp", "temp");
    output.close();

    IndexInput input = new ByteBuffersIndexInput(out.toDataInput(), "temp");

    CorruptIndexException e =
        expectThrows(CorruptIndexException.class, () -> CodecUtil.checksumEntireFile(input));
    assertTrue(
        e.getMessage(),
        e.getMessage()
            .contains(
                "misplaced codec footer (file truncated?): length=0 but footerLength==16 (resource"));

    e = expectThrows(CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(input));
    assertTrue(
        e.getMessage(),
        e.getMessage()
            .contains(
                "misplaced codec footer (file truncated?): length=0 but footerLength==16 (resource"));
  }

  public void testRetrieveChecksum() throws IOException {
    Directory dir = newDirectory();
    try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
      out.writeByte((byte) 42);
      CodecUtil.writeFooter(out);
    }
    try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
      CodecUtil.retrieveChecksum(in, in.length()); // no exception

      CorruptIndexException exception =
          expectThrows(
              CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(in, in.length() - 1));
      assertTrue(exception.getMessage().contains("too long"));
      assertArrayEquals(new Throwable[0], exception.getSuppressed());

      exception =
          expectThrows(
              CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(in, in.length() + 1));
      assertTrue(exception.getMessage().contains("truncated"));
      assertArrayEquals(new Throwable[0], exception.getSuppressed());
    }

    try (IndexOutput out = dir.createOutput("bar", IOContext.DEFAULT)) {
      for (int i = 0; i <= CodecUtil.footerLength(); ++i) {
        out.writeByte((byte) i);
      }
    }
    try (IndexInput in = dir.openInput("bar", IOContext.DEFAULT)) {
      CorruptIndexException exception =
          expectThrows(
              CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(in, in.length()));
      assertTrue(exception.getMessage().contains("codec footer mismatch"));
      assertArrayEquals(new Throwable[0], exception.getSuppressed());

      exception =
          expectThrows(
              CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(in, in.length() - 1));
      assertTrue(exception.getMessage().contains("too long"));

      exception =
          expectThrows(
              CorruptIndexException.class, () -> CodecUtil.retrieveChecksum(in, in.length() + 1));
      assertTrue(exception.getMessage().contains("truncated"));
    }

    dir.close();
  }
}
