package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.AttributeImpl;

/** 
 * This class wraps a Token and supplies a single attribute instance
 * where the delegate token can be replaced.
 * @deprecated Will be removed, when old TokenStream API is removed.
 */
final class TokenWrapper extends AttributeImpl 
                   implements Cloneable, TermAttribute, TypeAttribute, PositionIncrementAttribute,
                              FlagsAttribute, OffsetAttribute, PayloadAttribute {

  Token delegate;

  TokenWrapper() {
    this(new Token());
  }
  
  TokenWrapper(Token delegate) {
    this.delegate = delegate;
  }
  
  // TermAttribute:

  public String term() {
    return delegate.term();
  }

  public void setTermBuffer(char[] buffer, int offset, int length) {
    delegate.setTermBuffer(buffer, offset, length);
  }

  public void setTermBuffer(String buffer) {
    delegate.setTermBuffer(buffer);
  }

  public void setTermBuffer(String buffer, int offset, int length) {
    delegate.setTermBuffer(buffer, offset, length);
  }
  
  public char[] termBuffer() {
    return delegate.termBuffer();
  }

  public char[] resizeTermBuffer(int newSize) {
    return delegate.resizeTermBuffer(newSize);
  }

  public int termLength() {
    return delegate.termLength();
  }
  
  public void setTermLength(int length) {
    delegate.setTermLength(length);
  }
  
  // TypeAttribute:
  
  public String type() {
    return delegate.type();
  }

  public void setType(String type) {
    delegate.setType(type);
  }

  public void setPositionIncrement(int positionIncrement) {
    delegate.setPositionIncrement(positionIncrement);
  }

  public int getPositionIncrement() {
    return delegate.getPositionIncrement();
  }
  
  // FlagsAttribute
  
  public int getFlags() {
    return delegate.getFlags();
  }

  public void setFlags(int flags) {
    delegate.setFlags(flags);
  }
  
  // OffsetAttribute
  
  public int startOffset() {
    return delegate.startOffset();
  }

  public void setOffset(int startOffset, int endOffset) {
    delegate.setOffset(startOffset, endOffset);
  }
  
  public int endOffset() {
    return delegate.endOffset();
  }
  
  // PayloadAttribute
  
  public Payload getPayload() {
    return delegate.getPayload();
  }

  public void setPayload(Payload payload) {
    delegate.setPayload(payload);
  }
  
  // AttributeImpl

  public void clear() {
    delegate.clear();
  }

  public String toString() {
    return delegate.toString();
  }
  
  public int hashCode() {
    return delegate.hashCode();
  }
  
  public boolean equals(Object other) {
    if (other instanceof TokenWrapper) {
      return ((TokenWrapper) other).delegate.equals(this.delegate);
    }
    return false;
  }
  
  public Object clone() {
    return new TokenWrapper((Token) delegate.clone());
  }

  public void copyTo(AttributeImpl target) {
    if (target instanceof TokenWrapper) {
      ((TokenWrapper) target).delegate = (Token) this.delegate.clone();
    } else {
      this.delegate.copyTo(target);
    }
  }
}
