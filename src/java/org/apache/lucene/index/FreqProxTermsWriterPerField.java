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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.analysis.Token;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashConsumerPerField implements Comparable {

  final FreqProxTermsWriterPerThread perThread;
  final TermsHashPerField termsHashPerField;
  final FieldInfo fieldInfo;
  final DocumentsWriter.DocState docState;
  final DocInverter.FieldInvertState fieldState;
  boolean omitTf;

  public FreqProxTermsWriterPerField(TermsHashPerField termsHashPerField, FreqProxTermsWriterPerThread perThread, FieldInfo fieldInfo) {
    this.termsHashPerField = termsHashPerField;
    this.perThread = perThread;
    this.fieldInfo = fieldInfo;
    docState = termsHashPerField.docState;
    fieldState = termsHashPerField.fieldState;
    omitTf = fieldInfo.omitTf;
  }

  int getStreamCount() {
    if (fieldInfo.omitTf)
      return 1;
    else
      return 2;
  }

  void finish() {}

  boolean hasPayloads;

  void skippingLongTerm(Token t) throws IOException {}

  public int compareTo(Object other0) {
    FreqProxTermsWriterPerField other = (FreqProxTermsWriterPerField) other0;
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  void reset() {
    // Record, up front, whether our in-RAM format will be
    // with or without term freqs:
    omitTf = fieldInfo.omitTf;
  }

  boolean start(Fieldable[] fields, int count) {
    for(int i=0;i<count;i++)
      if (fields[i].isIndexed())
        return true;
    return false;
  }     

  final void writeProx(Token t, FreqProxTermsWriter.PostingList p, int proxCode) {
    final Payload payload = t.getPayload();    
    if (payload != null && payload.length > 0) {
      termsHashPerField.writeVInt(1, (proxCode<<1)|1);
      termsHashPerField.writeVInt(1, payload.length);
      termsHashPerField.writeBytes(1, payload.data, payload.offset, payload.length);
      hasPayloads = true;      
    } else
      termsHashPerField.writeVInt(1, proxCode<<1);
    p.lastPosition = fieldState.position;
  }

  final void newTerm(Token t, RawPostingList p0) {
    // First time we're seeing this term since the last
    // flush
    assert docState.testPoint("FreqProxTermsWriterPerField.newTerm start");
    FreqProxTermsWriter.PostingList p = (FreqProxTermsWriter.PostingList) p0;
    p.lastDocID = docState.docID;
    if (omitTf) {
      p.lastDocCode = docState.docID;
    } else {
      p.lastDocCode = docState.docID << 1;
      p.docFreq = 1;
      writeProx(t, p, fieldState.position);
    }
  }

  final void addTerm(Token t, RawPostingList p0) {

    assert docState.testPoint("FreqProxTermsWriterPerField.addTerm start");

    FreqProxTermsWriter.PostingList p = (FreqProxTermsWriter.PostingList) p0;

    assert omitTf || p.docFreq > 0;

    if (omitTf) {
      if (docState.docID != p.lastDocID) {
        assert docState.docID > p.lastDocID;
        termsHashPerField.writeVInt(0, p.lastDocCode);
        p.lastDocCode = docState.docID - p.lastDocID;
        p.lastDocID = docState.docID;
      }
    } else {
      if (docState.docID != p.lastDocID) {
        assert docState.docID > p.lastDocID;
        // Term not yet seen in the current doc but previously
        // seen in other doc(s) since the last flush

        // Now that we know doc freq for previous doc,
        // write it & lastDocCode
        if (1 == p.docFreq)
          termsHashPerField.writeVInt(0, p.lastDocCode|1);
        else {
          termsHashPerField.writeVInt(0, p.lastDocCode);
          termsHashPerField.writeVInt(0, p.docFreq);
        }
        p.docFreq = 1;
        p.lastDocCode = (docState.docID - p.lastDocID) << 1;
        p.lastDocID = docState.docID;
        writeProx(t, p, fieldState.position);
      } else {
        p.docFreq++;
        writeProx(t, p, fieldState.position-p.lastPosition);
      }
    }
  }

  public void abort() {}
}

