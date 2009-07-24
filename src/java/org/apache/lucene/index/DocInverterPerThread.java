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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/** This is a DocFieldConsumer that inverts each field,
 *  separately, from a Document, and accepts a
 *  InvertedTermsConsumer to process those terms. */

final class DocInverterPerThread extends DocFieldConsumerPerThread {
  final DocInverter docInverter;
  final InvertedDocConsumerPerThread consumer;
  final InvertedDocEndConsumerPerThread endConsumer;
  //TODO: change to SingleTokenTokenStream after Token was removed
  final SingleTokenTokenStream singleTokenTokenStream = new SingleTokenTokenStream();
  
  static class SingleTokenTokenStream extends TokenStream {
    TermAttribute termAttribute;
    OffsetAttribute offsetAttribute;
    
    SingleTokenTokenStream() {
      termAttribute = (TermAttribute) addAttribute(TermAttribute.class);
      offsetAttribute = (OffsetAttribute) addAttribute(OffsetAttribute.class);
    }
    
    public void reinit(String stringValue, int startOffset,  int endOffset) {
      termAttribute.setTermBuffer(stringValue);
      offsetAttribute.setOffset(startOffset, endOffset);
    }
    
    // this is a dummy, to not throw an UOE because this class does not implement any iteration method
    public boolean incrementToken() {
      throw new UnsupportedOperationException();
    }
  }
  
  final DocumentsWriter.DocState docState;

  final FieldInvertState fieldState = new FieldInvertState();

  // Used to read a string value for a field
  final ReusableStringReader stringReader = new ReusableStringReader();

  public DocInverterPerThread(DocFieldProcessorPerThread docFieldProcessorPerThread, DocInverter docInverter) {
    this.docInverter = docInverter;
    docState = docFieldProcessorPerThread.docState;
    consumer = docInverter.consumer.addThread(this);
    endConsumer = docInverter.endConsumer.addThread(this);
  }

  public void startDocument() throws IOException {
    consumer.startDocument();
    endConsumer.startDocument();
  }

  public DocumentsWriter.DocWriter finishDocument() throws IOException {
    // TODO: allow endConsumer.finishDocument to also return
    // a DocWriter
    endConsumer.finishDocument();
    return consumer.finishDocument();
  }

  void abort() {
    try {
      consumer.abort();
    } finally {
      endConsumer.abort();
    }
  }

  public DocFieldConsumerPerField addField(FieldInfo fi) {
    return new DocInverterPerField(this, fi);
  }
}
