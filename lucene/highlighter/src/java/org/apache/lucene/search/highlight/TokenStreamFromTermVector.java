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
package org.apache.lucene.search.highlight;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.UnicodeUtil;

/**
 * TokenStream created from a term vector field. The term vector requires positions and/or offsets (either). If you
 * want payloads add PayloadAttributeImpl (as you would normally) but don't assume the attribute is already added just
 * because you know the term vector has payloads, since the first call to incrementToken() will observe if you asked
 * for them and if not then won't get them.  This TokenStream supports an efficient {@link #reset()}, so there's
 * no need to wrap with a caching impl.
 * <p>
 * The implementation will create an array of tokens indexed by token position.  As long as there aren't massive jumps
 * in positions, this is fine.  And it assumes there aren't large numbers of tokens at the same position, since it adds
 * them to a linked-list per position in O(N^2) complexity.  When there aren't positions in the term vector, it divides
 * the startOffset by 8 to use as a temporary substitute. In that case, tokens with the same startOffset will occupy
 * the same final position; otherwise tokens become adjacent.
 *
 * @lucene.internal
 */
public final class TokenStreamFromTermVector extends TokenStream {

  private final Terms vector;

  private final CharTermAttribute termAttribute;

  private final PositionIncrementAttribute positionIncrementAttribute;

  private final int maxStartOffset;

  private OffsetAttribute offsetAttribute;//maybe null

  private PayloadAttribute payloadAttribute;//maybe null

  private CharsRefBuilder termCharsBuilder;//term data here

  private BytesRefArray payloadsBytesRefArray;//only used when payloadAttribute is non-null
  private BytesRefBuilder spareBytesRefBuilder;//only used when payloadAttribute is non-null

  private TokenLL firstToken = null; // the head of a linked-list

  private TokenLL incrementToken = null;

  private boolean initialized = false;//lazy

  /**
   * Constructor. The uninversion doesn't happen here; it's delayed till the first call to
   * {@link #incrementToken}.
   *
   * @param vector Terms that contains the data for
   *        creating the TokenStream. Must have positions and/or offsets.
   * @param maxStartOffset if a token's start offset exceeds this then the token is not added. -1 disables the limit.
   */
  public TokenStreamFromTermVector(Terms vector, int maxStartOffset) throws IOException {
    this.maxStartOffset = maxStartOffset < 0 ? Integer.MAX_VALUE : maxStartOffset;
    assert !hasAttribute(PayloadAttribute.class) : "AttributeFactory shouldn't have payloads *yet*";
    if (!vector.hasPositions() && !vector.hasOffsets()) {
      throw new IllegalArgumentException("The term vector needs positions and/or offsets.");
    }
    assert vector.hasFreqs();
    this.vector = vector;
    termAttribute = addAttribute(CharTermAttribute.class);
    positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
  }

  public Terms getTermVectorTerms() { return vector; }

  @Override
  public void reset() throws IOException {
    incrementToken = null;
    super.reset();
  }

  //We delay initialization because we can see which attributes the consumer wants, particularly payloads
  private void init() throws IOException {
    assert !initialized;
    short dpEnumFlags = PostingsEnum.POSITIONS;
    if (vector.hasOffsets()) {
      dpEnumFlags |= PostingsEnum.OFFSETS;
      offsetAttribute = addAttribute(OffsetAttribute.class);
    }
    if (vector.hasPayloads() && hasAttribute(PayloadAttribute.class)) {
      dpEnumFlags |= (PostingsEnum.OFFSETS | PostingsEnum.PAYLOADS);//must ask for offsets too
      payloadAttribute = getAttribute(PayloadAttribute.class);
      payloadsBytesRefArray = new BytesRefArray(Counter.newCounter());
      spareBytesRefBuilder = new BytesRefBuilder();
    }

    // We put term data here
    termCharsBuilder = new CharsRefBuilder();
    termCharsBuilder.grow((int) (vector.size() * 7));//7 is over-estimate of average term len

    // Step 1: iterate termsEnum and create a token, placing into an array of tokens by position

    TokenLL[] positionedTokens = initTokensArray();

    int lastPosition = -1;

    final TermsEnum termsEnum = vector.iterator();
    BytesRef termBytesRef;
    PostingsEnum dpEnum = null;
    CharsRefBuilder tempCharsRefBuilder = new CharsRefBuilder();//only for UTF8->UTF16 call
    //int sumFreq = 0;
    while ((termBytesRef = termsEnum.next()) != null) {
      //Grab the term (in same way as BytesRef.utf8ToString() but we don't want a String obj)
      // note: if term vectors supported seek by ord then we might just keep an int and seek by ord on-demand
      tempCharsRefBuilder.grow(termBytesRef.length);
      final int termCharsLen = UnicodeUtil.UTF8toUTF16(termBytesRef, tempCharsRefBuilder.chars());
      final int termCharsOff = termCharsBuilder.length();
      termCharsBuilder.append(tempCharsRefBuilder.chars(), 0, termCharsLen);

      dpEnum = termsEnum.postings(dpEnum, dpEnumFlags);
      assert dpEnum != null; // presumably checked by TokenSources.hasPositions earlier
      dpEnum.nextDoc();
      final int freq = dpEnum.freq();
      //sumFreq += freq;
      for (int j = 0; j < freq; j++) {
        int pos = dpEnum.nextPosition();
        TokenLL token = new TokenLL();
        token.termCharsOff = termCharsOff;
        token.termCharsLen = (short) Math.min(termCharsLen, Short.MAX_VALUE);
        if (offsetAttribute != null) {
          token.startOffset = dpEnum.startOffset();
          if (token.startOffset > maxStartOffset) {
            continue;//filter this token out; exceeds threshold
          }
          token.endOffsetInc = (short) Math.min(dpEnum.endOffset() - token.startOffset, Short.MAX_VALUE);
          if (pos == -1) {
            pos = token.startOffset >> 3;//divide by 8
          }
        }

        if (payloadAttribute != null) {
          final BytesRef payload = dpEnum.getPayload();
          token.payloadIndex = payload == null ? -1 : payloadsBytesRefArray.append(payload);
        }

        //Add token to an array indexed by position
        if (positionedTokens.length <= pos) {
          //grow, but not 2x since we think our original length estimate is close
          TokenLL[] newPositionedTokens = new TokenLL[(int)((pos + 1) * 1.5f)];
          System.arraycopy(positionedTokens, 0, newPositionedTokens, 0, lastPosition + 1);
          positionedTokens = newPositionedTokens;
        }
        positionedTokens[pos] = token.insertIntoSortedLinkedList(positionedTokens[pos]);

        lastPosition = Math.max(lastPosition, pos);
      }
    }

//    System.out.println(String.format(
//        "SumFreq: %5d Size: %4d SumFreq/size: %3.3f MaxPos: %4d MaxPos/SumFreq: %3.3f WastePct: %3.3f",
//        sumFreq, vector.size(), (sumFreq / (float)vector.size()), lastPosition, ((float)lastPosition)/sumFreq,
//        (originalPositionEstimate/(lastPosition + 1.0f))));

    // Step 2:  Link all Tokens into a linked-list and set position increments as we go

    int prevTokenPos = -1;
    TokenLL prevToken = null;
    for (int pos = 0; pos <= lastPosition; pos++) {
      TokenLL token = positionedTokens[pos];
      if (token == null) {
        continue;
      }
      //link
      if (prevToken != null) {
        assert prevToken.next == null;
        prevToken.next = token; //concatenate linked-list
      } else {
        assert firstToken == null;
        firstToken = token;
      }
      //set increments
      if (vector.hasPositions()) {
        token.positionIncrement = pos - prevTokenPos;
        while (token.next != null) {
          token = token.next;
          token.positionIncrement = 0;
        }
      } else {
        token.positionIncrement = 1;
        while (token.next != null) {
          prevToken = token;
          token = token.next;
          if (prevToken.startOffset == token.startOffset) {
            token.positionIncrement = 0;
          } else {
            token.positionIncrement = 1;
          }
        }
      }
      prevTokenPos = pos;
      prevToken = token;
    }

    initialized = true;
  }

  private TokenLL[] initTokensArray() throws IOException {
    // Estimate the number of position slots we need from term stats.  We use some estimation factors taken from
    //  Wikipedia that reduce the likelihood of needing to expand the array.
    int sumTotalTermFreq = (int) vector.getSumTotalTermFreq();
    assert sumTotalTermFreq != -1;

    final int originalPositionEstimate = (int) (sumTotalTermFreq * 1.5);//less than 1 in 10 docs exceed this

    // This estimate is based on maxStartOffset. Err on the side of this being larger than needed.
    final int offsetLimitPositionEstimate = (int) (maxStartOffset / 5.0);

    // Take the smaller of the two estimates, but no smaller than 64
    return new TokenLL[Math.max(64, Math.min(originalPositionEstimate, offsetLimitPositionEstimate))];
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (incrementToken == null) {
      if (!initialized) {
        init();
        assert initialized;
      }
      incrementToken = firstToken;
      if (incrementToken == null) {
        return false;
      }
    } else if (incrementToken.next != null) {
      incrementToken = incrementToken.next;
    } else {
      return false;
    }
    clearAttributes();
    termAttribute.copyBuffer(termCharsBuilder.chars(), incrementToken.termCharsOff, incrementToken.termCharsLen);
    positionIncrementAttribute.setPositionIncrement(incrementToken.positionIncrement);
    if (offsetAttribute != null) {
      offsetAttribute.setOffset(incrementToken.startOffset, incrementToken.startOffset + incrementToken.endOffsetInc);
    }
    if (payloadAttribute != null) {
      if (incrementToken.payloadIndex == -1) {
        payloadAttribute.setPayload(null);
      } else {
        payloadAttribute.setPayload(payloadsBytesRefArray.get(spareBytesRefBuilder, incrementToken.payloadIndex));
      }
    }
    return true;
  }

  private static class TokenLL {
    // This class should weigh 32 bytes, including object header

    int termCharsOff; // see termCharsBuilder
    short termCharsLen;

    int positionIncrement;
    int startOffset;
    short endOffsetInc; // add to startOffset to get endOffset
    int payloadIndex;

    TokenLL next;

    /** Given the head of a linked-list (possibly null) this inserts the token at the correct
     * spot to maintain the desired order, and returns the head (which could be this token if it's the smallest).
     * O(N^2) complexity but N should be a handful at most.
     */
    TokenLL insertIntoSortedLinkedList(final TokenLL head) {
      assert next == null;
      if (head == null) {
        return this;
      } else if (this.compareOffsets(head) <= 0) {
        this.next = head;
        return this;
      }
      TokenLL prev = head;
      while (prev.next != null && this.compareOffsets(prev.next) > 0) {
        prev = prev.next;
      }
      this.next = prev.next;
      prev.next = this;
      return head;
    }

    /** by startOffset then endOffset */
    int compareOffsets(TokenLL tokenB) {
      int cmp = Integer.compare(this.startOffset, tokenB.startOffset);
      if (cmp == 0) {
        cmp = Short.compare(this.endOffsetInc, tokenB.endOffsetInc);
      }
      return cmp;
    }
  }
}
