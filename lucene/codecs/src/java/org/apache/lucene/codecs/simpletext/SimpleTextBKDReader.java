package org.apache.lucene.codecs.simpletext;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDReader;

import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.BLOCK_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.BLOCK_DOC_ID;
import static org.apache.lucene.codecs.simpletext.SimpleTextDimensionalWriter.BLOCK_VALUE;

class SimpleTextBKDReader extends BKDReader {

  public SimpleTextBKDReader(IndexInput datIn, int numDims, int maxPointsInLeafNode, int bytesPerDim, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    super(datIn, numDims, maxPointsInLeafNode, bytesPerDim, leafBlockFPs, splitPackedValues);
  }

  @Override
  protected void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    in.seek(blockFP);
    readLine(in, scratch);
    int count = parseInt(scratch, BLOCK_COUNT);
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      visitor.visit(parseInt(scratch, BLOCK_DOC_ID));
    }
  }

  @Override
  protected int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    in.seek(blockFP);
    readLine(in, scratch);
    int count = parseInt(scratch, BLOCK_COUNT);
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      docIDs[i] = parseInt(scratch, BLOCK_DOC_ID);
    }
    return count;
  }

  @Override
  protected void visitDocValues(byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
    assert scratchPackedValue.length == packedBytesLength;
    BytesRefBuilder scratch = new BytesRefBuilder();
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      assert startsWith(scratch, BLOCK_VALUE);
      BytesRef br = SimpleTextUtil.fromBytesRefString(stripPrefix(scratch, BLOCK_VALUE));
      assert br.length == packedBytesLength;
      System.arraycopy(br.bytes, br.offset, scratchPackedValue, 0, packedBytesLength);
      visitor.visit(docIDs[i], scratchPackedValue);
    }
  }

  private int parseInt(BytesRefBuilder scratch, BytesRef prefix) {
    assert startsWith(scratch, prefix);
    return Integer.parseInt(stripPrefix(scratch, prefix));
  }

  private String stripPrefix(BytesRefBuilder scratch, BytesRef prefix) {
    return new String(scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
  }

  private boolean startsWith(BytesRefBuilder scratch, BytesRef prefix) {
    return StringHelper.startsWith(scratch.get(), prefix);
  }

  private void readLine(IndexInput in, BytesRefBuilder scratch) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
  }
}
