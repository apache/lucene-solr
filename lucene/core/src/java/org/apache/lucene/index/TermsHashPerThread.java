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

import java.io.IOException;

final class TermsHashPerThread extends InvertedDocConsumerPerThread {

  final TermsHash termsHash;
  final TermsHashConsumerPerThread consumer;
  final TermsHashPerThread nextPerThread;

  final CharBlockPool charPool;
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final boolean primary;
  final DocumentsWriter.DocState docState;

  public TermsHashPerThread(DocInverterPerThread docInverterPerThread, final TermsHash termsHash, final TermsHash nextTermsHash, final TermsHashPerThread primaryPerThread) {
    docState = docInverterPerThread.docState;

    this.termsHash = termsHash;
    this.consumer = termsHash.consumer.addThread(this);

    if (nextTermsHash != null) {
      // We are primary
      charPool = new CharBlockPool(termsHash.docWriter);
      primary = true;
    } else {
      charPool = primaryPerThread.charPool;
      primary = false;
    }

    intPool = new IntBlockPool(termsHash.docWriter);
    bytePool = new ByteBlockPool(termsHash.docWriter.byteBlockAllocator);

    if (nextTermsHash != null)
      nextPerThread = nextTermsHash.addThread(docInverterPerThread, this);
    else
      nextPerThread = null;
  }

  @Override
  InvertedDocConsumerPerField addField(DocInverterPerField docInverterPerField, final FieldInfo fieldInfo) {
    return new TermsHashPerField(docInverterPerField, this, nextPerThread, fieldInfo);
  }

  @Override
  synchronized public void abort() {
    reset(true);
    try {
      consumer.abort();
    } finally {
      if (nextPerThread != null) {
        nextPerThread.abort();
      }
    }
  }

  @Override
  public void startDocument() throws IOException {
    consumer.startDocument();
    if (nextPerThread != null)
      nextPerThread.consumer.startDocument();
  }

  @Override
  public DocumentsWriter.DocWriter finishDocument() throws IOException {
    final DocumentsWriter.DocWriter doc = consumer.finishDocument();

    final DocumentsWriter.DocWriter doc2;
    if (nextPerThread != null)
      doc2 = nextPerThread.consumer.finishDocument();
    else
      doc2 = null;
    if (doc == null)
      return doc2;
    else {
      doc.setNext(doc2);
      return doc;
    }
  }

  // Clear all state
  void reset(boolean recyclePostings) {
    intPool.reset();
    bytePool.reset();

    if (primary)
      charPool.reset();
  }
}
