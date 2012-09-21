package org.apache.lucene.codecs.lucene40;

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
import java.util.Collection;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

/**
 * Lucene 4.0 Live Documents Format.
 * <p>
 * <p>The .del file is optional, and only exists when a segment contains
 * deletions.</p>
 * <p>Although per-segment, this file is maintained exterior to compound segment
 * files.</p>
 * <p>Deletions (.del) --&gt; Format,Header,ByteCount,BitCount, Bits | DGaps (depending
 * on Format)</p>
 * <ul>
 *   <li>Format,ByteSize,BitCount --&gt; {@link DataOutput#writeInt Uint32}</li>
 *   <li>Bits --&gt; &lt;{@link DataOutput#writeByte Byte}&gt; <sup>ByteCount</sup></li>
 *   <li>DGaps --&gt; &lt;DGap,NonOnesByte&gt; <sup>NonzeroBytesCount</sup></li>
 *   <li>DGap --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>NonOnesByte --&gt; {@link DataOutput#writeByte Byte}</li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * </ul>
 * <p>Format is 1: indicates cleared DGaps.</p>
 * <p>ByteCount indicates the number of bytes in Bits. It is typically
 * (SegSize/8)+1.</p>
 * <p>BitCount indicates the number of bits that are currently set in Bits.</p>
 * <p>Bits contains one bit for each document indexed. When the bit corresponding
 * to a document number is cleared, that document is marked as deleted. Bit ordering
 * is from least to most significant. Thus, if Bits contains two bytes, 0x00 and
 * 0x02, then document 9 is marked as alive (not deleted).</p>
 * <p>DGaps represents sparse bit-vectors more efficiently than Bits. It is made
 * of DGaps on indexes of nonOnes bytes in Bits, and the nonOnes bytes themselves.
 * The number of nonOnes bytes in Bits (NonOnesBytesCount) is not stored.</p>
 * <p>For example, if there are 8000 bits and only bits 10,12,32 are cleared, DGaps
 * would be used:</p>
 * <p>(VInt) 1 , (byte) 20 , (VInt) 3 , (Byte) 1</p>
 */
public class Lucene40LiveDocsFormat extends LiveDocsFormat {

  /** Extension of deletes */
  static final String DELETES_EXTENSION = "del";

  /** Sole constructor. */
  public Lucene40LiveDocsFormat() {
  }
  
  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    BitVector bitVector = new BitVector(size);
    bitVector.invertAll();
    return bitVector;
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    final BitVector liveDocs = (BitVector) existing;
    return liveDocs.clone();
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentInfoPerCommit info, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.info.name, DELETES_EXTENSION, info.getDelGen());
    final BitVector liveDocs = new BitVector(dir, filename, context);
    assert liveDocs.count() == info.info.getDocCount() - info.getDelCount():
      "liveDocs.count()=" + liveDocs.count() + " info.docCount=" + info.info.getDocCount() + " info.getDelCount()=" + info.getDelCount();
    assert liveDocs.length() == info.info.getDocCount();
    return liveDocs;
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfoPerCommit info, int newDelCount, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.info.name, DELETES_EXTENSION, info.getNextDelGen());
    final BitVector liveDocs = (BitVector) bits;
    assert liveDocs.count() == info.info.getDocCount() - info.getDelCount() - newDelCount;
    assert liveDocs.length() == info.info.getDocCount();
    liveDocs.write(dir, filename, context);
  }

  @Override
  public void files(SegmentInfoPerCommit info, Collection<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.info.name, DELETES_EXTENSION, info.getDelGen()));
    }
  }
}
