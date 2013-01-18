package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;

/**
 * TokenStream from a canned list of binary (BytesRef-based)
 * tokens.
 */
public final class CannedBinaryTokenStream extends TokenStream {

  /** Represents a binary token. */
  public final static class BinaryToken {
    BytesRef term;
    int posInc;
    int posLen;
    int startOffset;
    int endOffset;

    public BinaryToken(BytesRef term) {
      this.term = term;
      this.posInc = 1;
      this.posLen = 1;
    }

    public BinaryToken(BytesRef term, int posInc, int posLen) {
      this.term = term;
      this.posInc = posInc;
      this.posLen = posLen;
    }
  }

  private final BinaryToken[] tokens;
  private int upto = 0;
  private final BinaryTermAttribute termAtt = addAttribute(BinaryTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /** An attribute extending {@link
   *  TermToBytesRefAttribute} but exposing {@link
   *  #setBytesRef} method. */
  public interface BinaryTermAttribute extends TermToBytesRefAttribute {

    /** Set the current binary value. */
    public void setBytesRef(BytesRef bytes);
  }

  /** Implementation for {@link BinaryTermAttribute}. */
  public final static class BinaryTermAttributeImpl extends AttributeImpl implements BinaryTermAttribute, TermToBytesRefAttribute {
    private final BytesRef bytes = new BytesRef();

    @Override
    public int fillBytesRef() {
      return bytes.hashCode();
    }
      
    @Override
    public BytesRef getBytesRef() {
      return bytes;
    }

    @Override
    public void setBytesRef(BytesRef bytes) {
      this.bytes.copyBytes(bytes);
    }
    
    @Override
    public void clear() {
    }

    @Override
    public boolean equals(Object other) {
      return other == this;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
    
    @Override
    public void copyTo(AttributeImpl target) {
      BinaryTermAttributeImpl other = (BinaryTermAttributeImpl) target;
      other.bytes.copyBytes(bytes);
    }
    
    @Override
    public BinaryTermAttributeImpl clone() {
      throw new UnsupportedOperationException();
    }
  }

  public CannedBinaryTokenStream(BinaryToken... tokens) {
    super();
    this.tokens = tokens;
  }
  
  @Override
  public boolean incrementToken() {
    if (upto < tokens.length) {
      final BinaryToken token = tokens[upto++];     
      // TODO: can we just capture/restoreState so
      // we get all attrs...?
      clearAttributes();      
      termAtt.setBytesRef(token.term);
      posIncrAtt.setPositionIncrement(token.posInc);
      posLengthAtt.setPositionLength(token.posLen);
      offsetAtt.setOffset(token.startOffset, token.endOffset);
      return true;
    } else {
      return false;
    }
  }
}
