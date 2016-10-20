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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocIdsWriter extends LuceneTestCase {

  public void testRandom() throws Exception {
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < 1000; ++iter) {
        int[] docIDs = new int[random().nextInt(5000)];
        final int bpv = TestUtil.nextInt(random(), 1, 32);
        for (int i = 0; i < docIDs.length; ++i) {
          docIDs[i] = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        test(dir, docIDs);
      }
    }
  }

  public void testSorted() throws Exception {
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < 1000; ++iter) {
        int[] docIDs = new int[random().nextInt(5000)];
        final int bpv = TestUtil.nextInt(random(), 1, 32);
        for (int i = 0; i < docIDs.length; ++i) {
          docIDs[i] = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
        }
        Arrays.sort(docIDs);
        test(dir, docIDs);
      }
    }
  }

  private void test(Directory dir, int[] ints) throws Exception {
    final long len;
    try(IndexOutput out = dir.createOutput("tmp", IOContext.DEFAULT)) {
      DocIdsWriter.writeDocIds(ints, 0, ints.length, out);
      len = out.getFilePointer();
      if (random().nextBoolean()) {
        out.writeLong(0); // garbage
      }
    }
    try (IndexInput in = dir.openInput("tmp", IOContext.READONCE)) {
      int[] read = new int[ints.length];
      DocIdsWriter.readInts(in, ints.length, read);
      assertArrayEquals(ints, read);
      assertEquals(len, in.getFilePointer());
    }
    try (IndexInput in = dir.openInput("tmp", IOContext.READONCE)) {
      int[] read = new int[ints.length];
      DocIdsWriter.readInts(in, ints.length, new IntersectVisitor() {
        int i = 0;
        @Override
        public void visit(int docID) throws IOException {
          read[i++] = docID;
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          throw new UnsupportedOperationException();
        }

      });
      assertArrayEquals(ints, read);
      assertEquals(len, in.getFilePointer());
    }
    dir.deleteFile("tmp");
  }

}
