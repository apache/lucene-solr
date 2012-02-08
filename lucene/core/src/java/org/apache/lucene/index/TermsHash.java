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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/** This class implements {@link InvertedDocConsumer}, which
 *  is passed each token produced by the analyzer on each
 *  field.  It stores these tokens in a hash table, and
 *  allocates separate byte streams per token.  Consumers of
 *  this class, eg {@link FreqProxTermsWriter} and {@link
 *  TermVectorsTermsWriter}, write their own byte streams
 *  under each term.
 */
final class TermsHash extends InvertedDocConsumer {

  final TermsHashConsumer consumer;
  final TermsHash nextTermsHash;
  final DocumentsWriter docWriter;

  boolean trackAllocations;

  public TermsHash(final DocumentsWriter docWriter, boolean trackAllocations, final TermsHashConsumer consumer, final TermsHash nextTermsHash) {
    this.docWriter = docWriter;
    this.consumer = consumer;
    this.nextTermsHash = nextTermsHash;
    this.trackAllocations = trackAllocations;
  }

  @Override
  InvertedDocConsumerPerThread addThread(DocInverterPerThread docInverterPerThread) {
    return new TermsHashPerThread(docInverterPerThread, this, nextTermsHash, null);
  }

  TermsHashPerThread addThread(DocInverterPerThread docInverterPerThread, TermsHashPerThread primaryPerThread) {
    return new TermsHashPerThread(docInverterPerThread, this, nextTermsHash, primaryPerThread);
  }

  @Override
  void setFieldInfos(FieldInfos fieldInfos) {
    this.fieldInfos = fieldInfos;
    consumer.setFieldInfos(fieldInfos);
  }

  @Override
  public void abort() {
    try {
      consumer.abort();
    } finally {
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  @Override
  synchronized void flush(Map<InvertedDocConsumerPerThread,Collection<InvertedDocConsumerPerField>> threadsAndFields, final SegmentWriteState state) throws IOException {
    Map<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> childThreadsAndFields = new HashMap<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>>();
    Map<InvertedDocConsumerPerThread,Collection<InvertedDocConsumerPerField>> nextThreadsAndFields;

    if (nextTermsHash != null)
      nextThreadsAndFields = new HashMap<InvertedDocConsumerPerThread,Collection<InvertedDocConsumerPerField>>();
    else
      nextThreadsAndFields = null;

    for (final Map.Entry<InvertedDocConsumerPerThread,Collection<InvertedDocConsumerPerField>> entry : threadsAndFields.entrySet()) {

      TermsHashPerThread perThread = (TermsHashPerThread) entry.getKey();

      Collection<InvertedDocConsumerPerField> fields = entry.getValue();

      Iterator<InvertedDocConsumerPerField> fieldsIt = fields.iterator();
      Collection<TermsHashConsumerPerField> childFields = new HashSet<TermsHashConsumerPerField>();
      Collection<InvertedDocConsumerPerField> nextChildFields;

      if (nextTermsHash != null)
        nextChildFields = new HashSet<InvertedDocConsumerPerField>();
      else
        nextChildFields = null;

      while(fieldsIt.hasNext()) {
        TermsHashPerField perField = (TermsHashPerField) fieldsIt.next();
        childFields.add(perField.consumer);
        if (nextTermsHash != null)
          nextChildFields.add(perField.nextPerField);
      }

      childThreadsAndFields.put(perThread.consumer, childFields);
      if (nextTermsHash != null)
        nextThreadsAndFields.put(perThread.nextPerThread, nextChildFields);
    }
    
    consumer.flush(childThreadsAndFields, state);

    if (nextTermsHash != null)
      nextTermsHash.flush(nextThreadsAndFields, state);
  }

  @Override
  synchronized public boolean freeRAM() {
    return false;
  }
}
