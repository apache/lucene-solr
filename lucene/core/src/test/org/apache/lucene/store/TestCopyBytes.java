package org.apache.lucene.store;

/**
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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import org.junit.Test;

public class TestCopyBytes extends LuceneTestCase {
  
  private byte value(int idx) {
    return (byte) ((idx % 256) * (1 + (idx / 256)));
  }
  
  @Test
  public void testCopyBytes() throws Exception {
    int num = atLeast(10);
    for (int iter = 0; iter < num; iter++) {
      Directory dir = newDirectory();
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " dir=" + dir);
      }
      
      // make random file
      IndexOutput out = dir.createOutput("test", newIOContext(random()));
      byte[] bytes = new byte[_TestUtil.nextInt(random(), 1, 77777)];
      final int size = _TestUtil.nextInt(random(), 1, 1777777);
      int upto = 0;
      int byteUpto = 0;
      while (upto < size) {
        bytes[byteUpto++] = value(upto);
        upto++;
        if (byteUpto == bytes.length) {
          out.writeBytes(bytes, 0, bytes.length);
          byteUpto = 0;
        }
      }
      
      out.writeBytes(bytes, 0, byteUpto);
      assertEquals(size, out.getFilePointer());
      out.close();
      assertEquals(size, dir.fileLength("test"));
      
      // copy from test -> test2
      final IndexInput in = dir.openInput("test", newIOContext(random()));
      
      out = dir.createOutput("test2", newIOContext(random()));
      
      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          out.writeByte(in.readByte());
          upto++;
        } else {
          final int chunk = Math.min(
              _TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          out.copyBytes(in, chunk);
          upto += chunk;
        }
      }
      assertEquals(size, upto);
      out.close();
      in.close();
      
      // verify
      IndexInput in2 = dir.openInput("test2", newIOContext(random()));
      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          final byte v = in2.readByte();
          assertEquals(value(upto), v);
          upto++;
        } else {
          final int limit = Math.min(
              _TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          in2.readBytes(bytes, 0, limit);
          for (int byteIdx = 0; byteIdx < limit; byteIdx++) {
            assertEquals(value(upto), bytes[byteIdx]);
            upto++;
          }
        }
      }
      in2.close();
      
      dir.deleteFile("test");
      dir.deleteFile("test2");
      
      dir.close();
    }
  }
  
  // LUCENE-3541
  public void testCopyBytesWithThreads() throws Exception {
    int datalen = _TestUtil.nextInt(random(), 101, 10000);
    byte data[] = new byte[datalen];
    random().nextBytes(data);
    
    Directory d = newDirectory();
    IndexOutput output = d.createOutput("data", IOContext.DEFAULT);
    output.writeBytes(data, 0, datalen);
    output.close();
    
    IndexInput input = d.openInput("data", IOContext.DEFAULT);
    IndexOutput outputHeader = d.createOutput("header", IOContext.DEFAULT);
    // copy our 100-byte header
    input.copyBytes(outputHeader, 100);
    outputHeader.close();
    
    // now make N copies of the remaining bytes
    CopyThread copies[] = new CopyThread[10];
    for (int i = 0; i < copies.length; i++) {
      copies[i] = new CopyThread((IndexInput) input.clone(), d.createOutput("copy" + i, IOContext.DEFAULT));
    }
    
    for (int i = 0; i < copies.length; i++) {
      copies[i].start();
    }
    
    for (int i = 0; i < copies.length; i++) {
      copies[i].join();
    }
    
    for (int i = 0; i < copies.length; i++) {
      IndexInput copiedData = d.openInput("copy" + i, IOContext.DEFAULT);
      byte[] dataCopy = new byte[datalen];
      System.arraycopy(data, 0, dataCopy, 0, 100); // copy the header for easy testing
      copiedData.readBytes(dataCopy, 100, datalen-100);
      assertArrayEquals(data, dataCopy);
      copiedData.close();
    }
    input.close();
    d.close();
    
  }
  
  static class CopyThread extends Thread {
    final IndexInput src;
    final IndexOutput dst;
    
    CopyThread(IndexInput src, IndexOutput dst) {
      this.src = src;
      this.dst = dst;
    }

    @Override
    public void run() {
      try {
        src.copyBytes(dst, src.length()-100);
        dst.close();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
