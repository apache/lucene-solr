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

import org.apache.lucene.util.UnicodeUtil;

final class TermVectorsTermsWriterPerThread extends TermsHashConsumerPerThread {

  final TermVectorsTermsWriter termsWriter;
  final TermsHashPerThread termsHashPerThread;
  final DocumentsWriter.DocState docState;

  TermVectorsTermsWriter.PerDoc doc;

  public TermVectorsTermsWriterPerThread(TermsHashPerThread termsHashPerThread, TermVectorsTermsWriter termsWriter) {
    this.termsWriter = termsWriter;
    this.termsHashPerThread = termsHashPerThread;
    docState = termsHashPerThread.docState;
  }
  
  // Used by perField when serializing the term vectors
  final ByteSliceReader vectorSliceReader = new ByteSliceReader();

  final UnicodeUtil.UTF8Result utf8Results[] = {new UnicodeUtil.UTF8Result(),
                                                new UnicodeUtil.UTF8Result()};

  @Override
  public void startDocument() {
    assert clearLastVectorFieldName();
    if (doc != null) {
      doc.reset();
      doc.docID = docState.docID;
    }
  }

  @Override
  public DocumentsWriter.DocWriter finishDocument() {
    try {
      return doc;
    } finally {
      doc = null;
    }
  }

  @Override
  public TermsHashConsumerPerField addField(TermsHashPerField termsHashPerField, FieldInfo fieldInfo) {
    return new TermVectorsTermsWriterPerField(termsHashPerField, this, fieldInfo);
  }

  @Override
  public void abort() {
    if (doc != null) {
      doc.abort();
      doc = null;
    }
  }

  // Called only by assert
  final boolean clearLastVectorFieldName() {
    lastVectorFieldName = null;
    return true;
  }

  // Called only by assert
  String lastVectorFieldName;
  final boolean vectorFieldsInOrder(FieldInfo fi) {
    try {
      if (lastVectorFieldName != null)
        return lastVectorFieldName.compareTo(fi.name) < 0;
      else
        return true;
    } finally {
      lastVectorFieldName = fi.name;
    }
  }
}
