package org.apache.lucene.index;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;

/** tests for codecutil methods */
public class TestCodecUtil extends LuceneTestCase {
  
  public void testHeaderLength() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    output.close();
    
    IndexInput input = new RAMInputStream("file", file);
    input.seek(CodecUtil.headerLength("FooBar"));
    assertEquals("this is the data", input.readString());
    input.close();
  }
  
  public void testWriteTooLongHeader() throws Exception {
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      tooLong.append('a');
    }
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    try {
      CodecUtil.writeHeader(output, tooLong.toString(), 5);
      fail("didn't get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  public void testWriteNonAsciiHeader() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    try {
      CodecUtil.writeHeader(output, "\u1234", 5);
      fail("didn't get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  public void testReadHeaderWrongMagic() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    output.writeInt(1234);
    output.close();
    
    IndexInput input = new RAMInputStream("file", file);
    try {
      CodecUtil.checkHeader(input, "bogus", 1, 1);
      fail("didn't get expected exception");
    } catch (CorruptIndexException expected) {
      // expected
    }
  }
  
  public void testChecksumEntireFile() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();
    
    IndexInput input = new RAMInputStream("file", file);
    CodecUtil.checksumEntireFile(input);
    input.close();
  }
  
  public void testCheckFooterValid() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();
    
    ChecksumIndexInput input = new BufferedChecksumIndexInput(new RAMInputStream("file", file));
    Exception mine = new RuntimeException("fake exception");
    try {
      CodecUtil.checkFooter(input, mine);
      fail("didn't get expected exception");
    } catch (RuntimeException expected) {
      assertEquals("fake exception", expected.getMessage());
      Throwable suppressed[] = expected.getSuppressed();
      assertEquals(1, suppressed.length);
      assertTrue(suppressed[0].getMessage().contains("checksum passed"));
    }
    input.close();
  }
  
  public void testCheckFooterValidAtFooter() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();
    
    ChecksumIndexInput input = new BufferedChecksumIndexInput(new RAMInputStream("file", file));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    Exception mine = new RuntimeException("fake exception");
    try {
      CodecUtil.checkFooter(input, mine);
      fail("didn't get expected exception");
    } catch (RuntimeException expected) {
      assertEquals("fake exception", expected.getMessage());
      Throwable suppressed[] = expected.getSuppressed();
      assertEquals(1, suppressed.length);
      assertTrue(suppressed[0].getMessage().contains("checksum passed"));
    }
    input.close();
  }
  
  public void testCheckFooterValidPastFooter() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    CodecUtil.writeFooter(output);
    output.close();
    
    ChecksumIndexInput input = new BufferedChecksumIndexInput(new RAMInputStream("file", file));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    // bogusly read a byte too far (can happen)
    input.readByte();
    Exception mine = new RuntimeException("fake exception");
    try {
      CodecUtil.checkFooter(input, mine);
      fail("didn't get expected exception");
    } catch (RuntimeException expected) {
      assertEquals("fake exception", expected.getMessage());
      Throwable suppressed[] = expected.getSuppressed();
      assertEquals(1, suppressed.length);
      assertTrue(suppressed[0].getMessage().contains("checksum status indeterminate"));
    }
    input.close();
  }
  
  public void testCheckFooterInvalid() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(output, "FooBar", 5);
    output.writeString("this is the data");
    output.writeInt(CodecUtil.FOOTER_MAGIC);
    output.writeInt(0);
    output.writeLong(1234567); // write a bogus checksum
    output.close();
    
    ChecksumIndexInput input = new BufferedChecksumIndexInput(new RAMInputStream("file", file));
    CodecUtil.checkHeader(input, "FooBar", 5, 5);
    assertEquals("this is the data", input.readString());
    Exception mine = new RuntimeException("fake exception");
    try {
      CodecUtil.checkFooter(input, mine);
      fail("didn't get expected exception");
    } catch (RuntimeException expected) {
      assertEquals("fake exception", expected.getMessage());
      Throwable suppressed[] = expected.getSuppressed();
      assertEquals(1, suppressed.length);
      assertTrue(suppressed[0].getMessage().contains("checksum failed"));
    }
    input.close();
  }
  
  public void testSegmentHeaderLength() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    CodecUtil.writeIndexHeader(output, "FooBar", 5, StringHelper.randomId(), "xyz");
    output.writeString("this is the data");
    output.close();
    
    IndexInput input = new RAMInputStream("file", file);
    input.seek(CodecUtil.indexHeaderLength("FooBar", "xyz"));
    assertEquals("this is the data", input.readString());
    input.close();
  }
  
  public void testWriteTooLongSuffix() throws Exception {
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 256; i++) {
      tooLong.append('a');
    }
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    try {
      CodecUtil.writeIndexHeader(output, "foobar", 5, StringHelper.randomId(), tooLong.toString());
      fail("didn't get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
  
  public void testWriteVeryLongSuffix() throws Exception {
    StringBuilder justLongEnough = new StringBuilder();
    for (int i = 0; i < 255; i++) {
      justLongEnough.append('a');
    }
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    byte[] id = StringHelper.randomId();
    CodecUtil.writeIndexHeader(output, "foobar", 5, id, justLongEnough.toString());
    output.close();
    
    IndexInput input = new RAMInputStream("file", file);
    CodecUtil.checkIndexHeader(input, "foobar", 5, 5, id, justLongEnough.toString());
    assertEquals(input.getFilePointer(), input.length());
    assertEquals(input.getFilePointer(), CodecUtil.indexHeaderLength("foobar", justLongEnough.toString()));
    input.close();
  }
  
  public void testWriteNonAsciiSuffix() throws Exception {
    RAMFile file = new RAMFile();
    IndexOutput output = new RAMOutputStream(file, true);
    try {
      CodecUtil.writeIndexHeader(output, "foobar", 5, StringHelper.randomId(), "\u1234");
      fail("didn't get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
