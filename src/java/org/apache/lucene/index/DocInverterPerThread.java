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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;

/** This is a DocFieldConsumer that inverts each field,
 *  separately, from a Document, and accepts a
 *  InvertedTermsConsumer to process those terms. */

final class DocInverterPerThread extends DocFieldConsumerPerThread {
  final DocInverter docInverter;
  final InvertedDocConsumerPerThread consumer;
  final InvertedDocEndConsumerPerThread endConsumer;
  final Token localToken = new Token();
  //TODO: change to SingleTokenTokenStream after Token was removed
  final SingleTokenTokenStream singleTokenTokenStream = new SingleTokenTokenStream();
  final BackwardsCompatibilityStream localTokenStream = new BackwardsCompatibilityStream();
  
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
  }
  
  /** This stream wrapper is only used to maintain backwards compatibility with the
   *  old TokenStream API and can be removed in Lucene 3.0
   * @deprecated 
   */
  static class BackwardsCompatibilityStream extends TokenStream {
    private Token token;
      
    TermAttribute termAttribute = new TermAttribute() {
      public String term() {
        return token.term();
      }
      
      public char[] termBuffer() {
        return token.termBuffer();
      }
      
      public int termLength() {
        return token.termLength();
      }
    };
    OffsetAttribute offsetAttribute = new OffsetAttribute() {
      public int startOffset() {
        return token.startOffset();
      }

      public int endOffset() {
        return token.endOffset();
      }
    };
    
    PositionIncrementAttribute positionIncrementAttribute = new PositionIncrementAttribute() {
      public int getPositionIncrement() {
        return token.getPositionIncrement();
      }
    };
    
    FlagsAttribute flagsAttribute = new FlagsAttribute() {
      public int getFlags() {
        return token.getFlags();
      }
    };
    
    PayloadAttribute payloadAttribute = new PayloadAttribute() {
      public Payload getPayload() {
        return token.getPayload();
      }
    };
    
    TypeAttribute typeAttribute = new TypeAttribute() {
      public String type() {
        return token.type();
      }
    };
    
    BackwardsCompatibilityStream() {
      attributes.put(TermAttribute.class, termAttribute);
      attributes.put(OffsetAttribute.class, offsetAttribute);
      attributes.put(PositionIncrementAttribute.class, positionIncrementAttribute);
      attributes.put(FlagsAttribute.class, flagsAttribute);
      attributes.put(PayloadAttribute.class, payloadAttribute);
      attributes.put(TypeAttribute.class, typeAttribute);
    }
            
    public void set(Token token) {
      this.token = token;
    }
  };
  
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
