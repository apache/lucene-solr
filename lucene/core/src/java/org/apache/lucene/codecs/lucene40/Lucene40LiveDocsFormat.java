package org.apache.lucene.codecs.lucene40;

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
import java.util.Set;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

public class Lucene40LiveDocsFormat extends LiveDocsFormat {

  /** Extension of deletes */
  static final String DELETES_EXTENSION = "del";
  
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
  public Bits readLiveDocs(Directory dir, SegmentInfo info, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen());
    final BitVector liveDocs = new BitVector(dir, filename, context);
    assert liveDocs.count() == info.docCount - info.getDelCount();
    assert liveDocs.length() == info.docCount;
    return liveDocs;
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException {
    String filename = IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen());
    final BitVector liveDocs = (BitVector) bits;
    assert liveDocs.count() == info.docCount - info.getDelCount();
    assert liveDocs.length() == info.docCount;
    liveDocs.write(dir, filename, context);
  }

  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.name, DELETES_EXTENSION, info.getDelGen()));
    }
  }
}
