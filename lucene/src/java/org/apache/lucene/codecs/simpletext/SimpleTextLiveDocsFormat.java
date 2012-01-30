package org.apache.lucene.codecs.simpletext;

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
import java.util.BitSet;
import java.util.Set;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MutableBits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

/**
 * reads/writes plaintext live docs
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextLiveDocsFormat extends LiveDocsFormat {

  static final String LIVEDOCS_EXTENSION = "liv";
  
  final static BytesRef SIZE             = new BytesRef("size ");
  final static BytesRef DOC              = new BytesRef("  doc ");
  final static BytesRef END              = new BytesRef("END");
  
  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    return new SimpleTextMutableBits(size);
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    final SimpleTextBits bits = (SimpleTextBits) existing;
    return new SimpleTextMutableBits((BitSet)bits.bits.clone(), bits.size);
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentInfo info, IOContext context) throws IOException {
    assert info.hasDeletions();
    BytesRef scratch = new BytesRef();
    CharsRef scratchUTF16 = new CharsRef();
    
    String fileName = IndexFileNames.fileNameFromGeneration(info.name, LIVEDOCS_EXTENSION, info.getDelGen());
    IndexInput in = null;
    boolean success = false;
    try {
      in = dir.openInput(fileName, context);
      
      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch, SIZE);
      int size = parseIntAt(scratch, SIZE.length, scratchUTF16);
      
      BitSet bits = new BitSet(size);
      
      SimpleTextUtil.readLine(in, scratch);
      while (!scratch.equals(END)) {
        assert StringHelper.startsWith(scratch, DOC);
        int docid = parseIntAt(scratch, DOC.length, scratchUTF16);
        bits.set(docid);
        SimpleTextUtil.readLine(in, scratch);
      }
      
      success = true;
      return new SimpleTextBits(bits, size);
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }
  
  private int parseIntAt(BytesRef bytes, int offset, CharsRef scratch) throws IOException {
    UnicodeUtil.UTF8toUTF16(bytes.bytes, bytes.offset+offset, bytes.length-offset, scratch);
    return ArrayUtil.parseInt(scratch.chars, 0, scratch.length);
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException {
    BitSet set = ((SimpleTextBits) bits).bits;
    int size = bits.length();
    BytesRef scratch = new BytesRef();
    
    String fileName = IndexFileNames.fileNameFromGeneration(info.name, LIVEDOCS_EXTENSION, info.getDelGen());
    IndexOutput out = null;
    boolean success = false;
    try {
      out = dir.createOutput(fileName, context);
      SimpleTextUtil.write(out, SIZE);
      SimpleTextUtil.write(out, Integer.toString(size), scratch);
      SimpleTextUtil.writeNewline(out);
      
      for (int i = set.nextSetBit(0); i >= 0; i=set.nextSetBit(i + 1)) { 
        SimpleTextUtil.write(out, DOC);
        SimpleTextUtil.write(out, Integer.toString(i), scratch);
        SimpleTextUtil.writeNewline(out);
      }
      
      SimpleTextUtil.write(out, END);
      SimpleTextUtil.writeNewline(out);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(out);
      } else {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.name, LIVEDOCS_EXTENSION, info.getDelGen()));
    }
  }
  
  // read-only
  static class SimpleTextBits implements Bits {
    final BitSet bits;
    final int size;
    
    SimpleTextBits(BitSet bits, int size) {
      this.bits = bits;
      this.size = size;
    }
    
    @Override
    public boolean get(int index) {
      return bits.get(index);
    }

    @Override
    public int length() {
      return size;
    }
  }
  
  // read-write
  static class SimpleTextMutableBits extends SimpleTextBits implements MutableBits {

    SimpleTextMutableBits(int size) {
      this(new BitSet(size), size);
      bits.set(0, size);
    }
    
    SimpleTextMutableBits(BitSet bits, int size) {
      super(bits, size);
    }
    
    @Override
    public void clear(int bit) {
      bits.clear(bit);
    }
  }
}
