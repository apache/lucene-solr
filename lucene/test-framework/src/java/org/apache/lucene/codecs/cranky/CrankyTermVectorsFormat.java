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
package org.apache.lucene.codecs.cranky;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;

class CrankyTermVectorsFormat extends TermVectorsFormat {
  final TermVectorsFormat delegate;
  final Random random;
  
  CrankyTermVectorsFormat(TermVectorsFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return delegate.vectorsReader(directory, segmentInfo, fieldInfos, context);
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from TermVectorsFormat.vectorsWriter()");
    }
    return new CrankyTermVectorsWriter(delegate.vectorsWriter(directory, segmentInfo, context), random);
  }
  
  static class CrankyTermVectorsWriter extends TermVectorsWriter {
    final TermVectorsWriter delegate;
    final Random random;
    
    CrankyTermVectorsWriter(TermVectorsWriter delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }
    
    @Override
    public int merge(MergeState mergeState) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.merge()");
      }
      return super.merge(mergeState);
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.finish()");
      }
      delegate.finish(fis, numDocs);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.close()");
      }
    }

    // per doc/field methods: lower probability since they are invoked so many times.

    @Override
    public void startDocument(int numVectorFields) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.startDocument()");
      }
      delegate.startDocument(numVectorFields);
    }
    
    @Override
    public void finishDocument() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.finishDocument()");
      }
      delegate.finishDocument();
    }
    
    @Override
    public void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.startField()");
      }
      delegate.startField(info, numTerms, positions, offsets, payloads);
    }

    @Override
    public void finishField() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.finishField()");
      }
      delegate.finishField();
    }
    
    @Override
    public void startTerm(BytesRef term, int freq) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.startTerm()");
      }
      delegate.startTerm(term, freq);
    }

    @Override
    public void finishTerm() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.finishTerm()");
      }
      delegate.finishTerm();
    }
    
    @Override
    public void addPosition(int position, int startOffset, int endOffset, BytesRef payload) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.addPosition()");
      }
      delegate.addPosition(position, startOffset, endOffset, payload);
    }

    @Override
    public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermVectorsWriter.addProx()");
      }
      super.addProx(numProx, positions, offsets);
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.singleton(delegate);
    }
  }
}
