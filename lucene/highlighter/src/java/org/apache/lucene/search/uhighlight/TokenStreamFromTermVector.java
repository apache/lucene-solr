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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 *
 * @lucene.internal
 */
final class TokenStreamFromTermVector extends TokenStream {
  // note: differs from similar class in the standard highlighter. This one is optimized for sparse cases.

  /**
   * content length divided by distinct positions; an average of dense text.
   */
  private static final double AVG_CHARS_PER_POSITION = 6;

  private static final int INSERTION_SORT_THRESHOLD = 16;

  private final Terms vector;

  private final int filteredDocId;

  private final CharTermAttribute termAttribute;

  private final PositionIncrementAttribute positionIncrementAttribute;

  private final int offsetLength;

  private final float loadFactor;

  private OffsetAttribute offsetAttribute;//maybe null

  private PayloadAttribute payloadAttribute;//maybe null

  private CharsRefBuilder termCharsBuilder;//term data here

  private BytesRefArray payloadsBytesRefArray;//only used when payloadAttribute is non-null
  private BytesRefBuilder spareBytesRefBuilder;//only used when payloadAttribute is non-null

  private TokenLL firstToken = null; // the head of a linked-list

  private TokenLL incrementToken = null;

  private boolean initialized = false;//lazy

  public TokenStreamFromTermVector(Terms vector, int offsetLength) throws IOException {
    this(vector, 0, offsetLength, 1f);
  }

  /**
   * Constructor.
   *
   * @param vector        Terms that contains the data for
   *                      creating the TokenStream. Must have positions and/or offsets.
   * @param filteredDocId The docID we will process.
   * @param offsetLength  Supply the character length of the text being uninverted, or a lower value if you don't want
   *                      to invert text beyond an offset (in so doing this will act as a filter).  If you don't
   *                      know the length, pass -1.  In conjunction with {@code loadFactor}, it's used to
   *                      determine how many buckets to create during uninversion.
   *                      It's also used to filter out tokens with a start offset exceeding this value.
   * @param loadFactor    The percent of tokens from the original terms (by position count) that are
   *                      expected to be inverted.  If they are filtered (e.g.
   *                      {@link org.apache.lucene.index.FilterLeafReader.FilterTerms})
   *                      then consider using less than 1.0 to avoid wasting space.
   *                      1.0 means all, 1/64th would suggest 1/64th of all tokens coming from vector.
   */
  TokenStreamFromTermVector(Terms vector, int filteredDocId, int offsetLength, float loadFactor) throws IOException {
    super();
    this.filteredDocId = filteredDocId;
    this.offsetLength = offsetLength == Integer.MAX_VALUE ? -1 : offsetLength;
    if (loadFactor <= 0f || loadFactor > 1f) {
      throw new IllegalArgumentException("loadFactor should be > 0 and <= 1");
    }
    this.loadFactor = loadFactor;
    assert !hasAttribute(PayloadAttribute.class) : "AttributeFactory shouldn't have payloads *yet*";
    if (!vector.hasPositions() && !vector.hasOffsets()) {
      throw new IllegalArgumentException("The term vector needs positions and/or offsets.");
    }
    assert vector.hasFreqs();
    this.vector = vector;
    termAttribute = addAttribute(CharTermAttribute.class);
    positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
  }

  public Terms getTermVectorTerms() {
    return vector;
  }

  @Override
  public void reset() throws IOException {
    incrementToken = null;
    super.reset();
  }

  //We delay initialization because we can see which attributes the consumer wants, particularly payloads
  private void init() throws IOException {
    assert !initialized;
    int dpEnumFlags = 0;
    if (vector.hasOffsets()) {
      offsetAttribute = addAttribute(OffsetAttribute.class);
      dpEnumFlags |= PostingsEnum.OFFSETS;
    }
    if (vector.hasPayloads() && hasAttribute(PayloadAttribute.class)) {
      payloadAttribute = getAttribute(PayloadAttribute.class);
      payloadsBytesRefArray = new BytesRefArray(Counter.newCounter());
      spareBytesRefBuilder = new BytesRefBuilder();
      dpEnumFlags |= PostingsEnum.PAYLOADS;
    }

    // We put term data here
    termCharsBuilder = new CharsRefBuilder();
    termCharsBuilder.grow(initTotalTermCharLen());

    // Step 1: iterate termsEnum and create a token, placing into a bucketed array (given a load factor)

    final TokenLL[] tokenBuckets = initTokenBucketsArray();
    final double OFFSET_TO_BUCKET_IDX = loadFactor / AVG_CHARS_PER_POSITION;
    final double POSITION_TO_BUCKET_IDX = loadFactor;

    final TermsEnum termsEnum = vector.iterator();
    BytesRef termBytesRef;
    PostingsEnum dpEnum = null;
    final CharsRefBuilder tempCharsRefBuilder = new CharsRefBuilder();//only for UTF8->UTF16 call

    TERM_LOOP:
    while ((termBytesRef = termsEnum.next()) != null) {
      //Grab the term (in same way as BytesRef.utf8ToString() but we don't want a String obj)
      // note: if term vectors supported seek by ord then we might just keep an int and seek by ord on-demand
      tempCharsRefBuilder.grow(termBytesRef.length);
      final int termCharsLen = UnicodeUtil.UTF8toUTF16(termBytesRef, tempCharsRefBuilder.chars());
      final int termCharsOff = termCharsBuilder.length();
      termCharsBuilder.append(tempCharsRefBuilder.chars(), 0, termCharsLen);
      dpEnum = termsEnum.postings(dpEnum, dpEnumFlags);
      assert dpEnum != null; // presumably checked by TokenSources.hasPositions earlier
      int currentDocId = dpEnum.advance(filteredDocId);
      if (currentDocId != filteredDocId) {
        continue; //Not expected
      }
      final int freq = dpEnum.freq();
      for (int j = 0; j < freq; j++) {
        TokenLL token = new TokenLL();
        token.position = dpEnum.nextPosition(); // can be -1 if not in the TV
        token.termCharsOff = termCharsOff;
        token.termCharsLen = (short) Math.min(termCharsLen, Short.MAX_VALUE);
        // copy offset (if it's there) and compute bucketIdx
        int bucketIdx;
        if (offsetAttribute != null) {
          token.startOffset = dpEnum.startOffset();
          if (offsetLength >= 0 && token.startOffset > offsetLength) {
            continue TERM_LOOP;//filter this token out; exceeds threshold
          }
          token.endOffsetInc = (short) Math.min(dpEnum.endOffset() - token.startOffset, Short.MAX_VALUE);
          bucketIdx = (int) (token.startOffset * OFFSET_TO_BUCKET_IDX);
        } else {
          bucketIdx = (int) (token.position * POSITION_TO_BUCKET_IDX);
        }
        if (bucketIdx >= tokenBuckets.length) {
          bucketIdx = tokenBuckets.length - 1;
        }

        if (payloadAttribute != null) {
          final BytesRef payload = dpEnum.getPayload();
          token.payloadIndex = payload == null ? -1 : payloadsBytesRefArray.append(payload);
        }

        //Add token to the head of the bucket linked list
        token.next = tokenBuckets[bucketIdx];
        tokenBuckets[bucketIdx] = token;
      }
    }

    // Step 2:  Link all Tokens into a linked-list and sort all tokens at the same position

    firstToken = initLinkAndSortTokens(tokenBuckets);

    // If the term vector didn't have positions, synthesize them
    if (!vector.hasPositions() && firstToken != null) {
      TokenLL prevToken = firstToken;
      prevToken.position = 0;
      for (TokenLL token = prevToken.next; token != null; prevToken = token, token = token.next) {
        if (prevToken.startOffset == token.startOffset) {
          token.position = prevToken.position;
        } else {
          token.position = prevToken.position + 1;
        }
      }
    }

    initialized = true;
  }

  private static TokenLL initLinkAndSortTokens(TokenLL[] tokenBuckets) {
    TokenLL firstToken = null;
    List<TokenLL> scratchTokenArray = new ArrayList<>(); // declare here for re-use.  TODO use native array
    TokenLL prevToken = null;
    for (TokenLL tokenHead : tokenBuckets) {
      if (tokenHead == null) {
        continue;
      }
      //sort tokens at this position and link them; return the first
      TokenLL tokenTail;
      // just one token
      if (tokenHead.next == null) {
        tokenTail = tokenHead;
      } else {
        // add the linked list to a temporary array
        for (TokenLL cur = tokenHead; cur != null; cur = cur.next) {
          scratchTokenArray.add(cur);
        }
        // sort; and set tokenHead & tokenTail
        if (scratchTokenArray.size() < INSERTION_SORT_THRESHOLD) {
          // insertion sort by creating a linked list (leave scratchTokenArray alone)
          tokenHead = tokenTail = scratchTokenArray.get(0);
          tokenHead.next = null;
          for (int i = 1; i < scratchTokenArray.size(); i++) {
            TokenLL insertToken = scratchTokenArray.get(i);
            if (insertToken.compareTo(tokenHead) <= 0) {
              // takes the place of tokenHead
              insertToken.next = tokenHead;
              tokenHead = insertToken;
            } else {
              // goes somewhere after tokenHead
              for (TokenLL prev = tokenHead; true; prev = prev.next) {
                if (prev.next == null || insertToken.compareTo(prev.next) <= 0) {
                  if (prev.next == null) {
                    tokenTail = insertToken;
                  }
                  insertToken.next = prev.next;
                  prev.next = insertToken;
                  break;
                }
              }
            }
          }
        } else {
          Collections.sort(scratchTokenArray);
          // take back out and create a linked list
          TokenLL prev = tokenHead = scratchTokenArray.get(0);
          for (int i = 1; i < scratchTokenArray.size(); i++) {
            prev.next = scratchTokenArray.get(i);
            prev = prev.next;
          }
          tokenTail = prev;
          tokenTail.next = null;
        }
        scratchTokenArray.clear();//too bad ArrayList nulls it out; we don't actually need that
      }

      //link to previous
      if (prevToken != null) {
        assert prevToken.next == null;
        prevToken.next = tokenHead; //concatenate linked-list
        assert prevToken.compareTo(tokenHead) < 0 : "wrong offset / position ordering expectations";
      } else {
        assert firstToken == null;
        firstToken = tokenHead;
      }

      prevToken = tokenTail;
    }
    return firstToken;
  }

  private int initTotalTermCharLen() throws IOException {
    int guessNumTerms;
    if (vector.size() != -1) {
      guessNumTerms = (int) vector.size();
    } else if (offsetLength != -1) {
      guessNumTerms = (int) (offsetLength * 0.33);//guess 1/3rd
    } else {
      return 128;
    }
    return Math.max(64, (int) (guessNumTerms * loadFactor * 7.0));//7 is over-estimate of average term len
  }

  private TokenLL[] initTokenBucketsArray() throws IOException {
    // Estimate the number of non-empty positions (number of tokens, excluding same-position synonyms).
    int positionsEstimate;
    if (offsetLength == -1) { // no clue what the char length is.
      // Estimate the number of position slots we need from term stats based on Wikipedia.
      int sumTotalTermFreq = (int) vector.getSumTotalTermFreq();
      if (sumTotalTermFreq == -1) {//unfortunately term vectors seem to not have this stat
        int size = (int) vector.size();
        if (size == -1) {//doesn't happen with term vectors, it seems, but pick a default any way
          size = 128;
        }
        sumTotalTermFreq = (int) (size * 2.4);
      }
      positionsEstimate = (int) (sumTotalTermFreq * 1.5);//less than 1 in 10 docs exceed this
    } else {
      // guess number of token positions by this factor.
      positionsEstimate = (int) (offsetLength / AVG_CHARS_PER_POSITION);
    }
    // apply the load factor.
    return new TokenLL[Math.max(1, (int) (positionsEstimate * loadFactor))];
  }

  @Override
  public boolean incrementToken() throws IOException {
    int posInc;
    if (incrementToken == null) {
      if (!initialized) {
        init();
        assert initialized;
      }
      incrementToken = firstToken;
      if (incrementToken == null) {
        return false;
      }
      posInc = incrementToken.position + 1;//first token normally has pos 0; add 1 to get posInc
    } else if (incrementToken.next != null) {
      int lastPosition = incrementToken.position;
      incrementToken = incrementToken.next;
      posInc = incrementToken.position - lastPosition;
    } else {
      return false;
    }
    clearAttributes();
    termAttribute.copyBuffer(termCharsBuilder.chars(), incrementToken.termCharsOff, incrementToken.termCharsLen);

    positionIncrementAttribute.setPositionIncrement(posInc);
    if (offsetAttribute != null) {
      offsetAttribute.setOffset(incrementToken.startOffset, incrementToken.startOffset + incrementToken.endOffsetInc);
    }
    if (payloadAttribute != null && incrementToken.payloadIndex >= 0) {
      payloadAttribute.setPayload(payloadsBytesRefArray.get(spareBytesRefBuilder, incrementToken.payloadIndex));
    }
    return true;
  }

  private static class TokenLL implements Comparable<TokenLL> {
    // This class should weigh 32 bytes, including object header

    int termCharsOff; // see termCharsBuilder
    short termCharsLen;

    int position;
    int startOffset;
    short endOffsetInc; // add to startOffset to get endOffset
    int payloadIndex;

    TokenLL next;

    @Override
    public int compareTo(TokenLL tokenB) {
      int cmp = Integer.compare(this.position, tokenB.position);
      if (cmp == 0) {
        cmp = Integer.compare(this.startOffset, tokenB.startOffset);
        if (cmp == 0) {
          cmp = Short.compare(this.endOffsetInc, tokenB.endOffsetInc);
        }
      }
      return cmp;
    }
  }
}
