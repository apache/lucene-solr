package org.apache.lucene.index;

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

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

final class TermsHashPerThread extends InvertedDocConsumerPerThread {

  final TermsHash termsHash;
  final TermsHashConsumerPerThread consumer;
  final TermsHashPerThread nextPerThread; // the secondary is currently consumed by TermVectorsWriter 
  // see secondary entry point in TermsHashPerField#add(int)

  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final ByteBlockPool termBytePool;
  
  final boolean primary;
  final DocumentsWriter.DocState docState;
  // Used by perField to obtain terms from the analysis chain
  final BytesRef termBytesRef = new BytesRef(10);

  public TermsHashPerThread(DocInverterPerThread docInverterPerThread, final TermsHash termsHash, final TermsHash nextTermsHash, final TermsHashPerThread primaryPerThread) {
    docState = docInverterPerThread.docState;

    this.termsHash = termsHash;
    this.consumer = termsHash.consumer.addThread(this);

    intPool = new IntBlockPool(termsHash.docWriter);
    bytePool = new ByteBlockPool(termsHash.docWriter.byteBlockAllocator); // use the allocator from the docWriter which tracks the used bytes 
    primary = nextTermsHash != null;
    if (primary) {
      // We are primary
      termBytePool = bytePool;
      nextPerThread = nextTermsHash.addThread(docInverterPerThread, this); // this will be the primaryPerThread in the secondary
      assert nextPerThread != null;
    } else {
      assert primaryPerThread != null;
      termBytePool = primaryPerThread.bytePool; // we are secondary and share the byte pool with the primary 
      nextPerThread = null;
    }
  }

  @Override
  InvertedDocConsumerPerField addField(DocInverterPerField docInverterPerField, final FieldInfo fieldInfo) {
    return new TermsHashPerField(docInverterPerField, this, nextPerThread, fieldInfo);
  }

  @Override
  synchronized public void abort() {
    reset(true);
    consumer.abort();
    if (primary)
      nextPerThread.abort();
  }

  @Override
  public void startDocument() throws IOException {
    consumer.startDocument();
    if (primary)
      nextPerThread.consumer.startDocument();
  }

  @Override
  public DocumentsWriter.DocWriter finishDocument() throws IOException {
    final DocumentsWriter.DocWriter doc = consumer.finishDocument();
    final DocumentsWriter.DocWriter docFromSecondary = primary? nextPerThread.consumer.finishDocument():null;
    if (doc == null)
      return docFromSecondary;
    else {
      doc.setNext(docFromSecondary);
      return doc;
    }
  }

  // Clear all state
  void reset(boolean recyclePostings) {
    intPool.reset();
    bytePool.reset();
  }
}
