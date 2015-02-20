package org.apache.lucene.index;

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

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Iterates through the postings.
 *  NOTE: you must first call {@link #nextDoc} before using
 *  any of the per-doc methods. */
public abstract class PostingsEnum extends DocIdSetIterator {
  
  /**
   * Flag to pass to {@link TermsEnum#postings(Bits, PostingsEnum, int)} if you don't
   * require per-document postings in the returned enum.
   */
  public static final int NONE = 0x0;

  /** Flag to pass to {@link TermsEnum#postings(Bits, PostingsEnum, int)}
   *  if you require term frequencies in the returned enum. */
  public static final int FREQS = 0x1;

  /** Flag to pass to {@link TermsEnum#postings(Bits, PostingsEnum, int)}
   * if you require term positions in the returned enum. */
  public static final int POSITIONS = 0x3;
  
  /** Flag to pass to {@link TermsEnum#postings(Bits, PostingsEnum, int)}
   *  if you require offsets in the returned enum. */
  public static final int OFFSETS = 0x7;

  /** Flag to pass to  {@link TermsEnum#postings(Bits, PostingsEnum, int)}
   *  if you require payloads in the returned enum. */
  public static final int PAYLOADS = 0xB;

  /**
   * Flag to pass to {@link TermsEnum#postings(Bits, PostingsEnum, int)}
   * to get positions, payloads and offsets in the returned enum
   */
  public static final int ALL = POSITIONS | PAYLOADS;

  /**
   * Returns true if the passed in flags require positions to be indexed
   * @param flags the postings flags
   * @return true if the passed in flags require positions to be indexed
   */
  public static boolean requiresPositions(int flags) {
    return ((flags & POSITIONS) >= POSITIONS);
  }

  private AttributeSource atts = null;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PostingsEnum() {
  }

  /**
   * Returns term frequency in the current document, or 1 if the field was
   * indexed with {@link IndexOptions#DOCS}. Do not call this before
   * {@link #nextDoc} is first called, nor after {@link #nextDoc} returns
   * {@link DocIdSetIterator#NO_MORE_DOCS}.
   * 
   * <p>
   * <b>NOTE:</b> if the {@link PostingsEnum} was obtain with {@link #NONE},
   * the result of this method is undefined.
   */
  public abstract int freq() throws IOException;
  
  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }

  /**
   * Returns the next position, or -1 if positions were not indexed.
   * Calling this more than {@link #freq()} times is undefined.
   */
  public abstract int nextPosition() throws IOException;

  /** Returns start offset for the current position, or -1
   *  if offsets were not indexed. */
  public abstract int startOffset() throws IOException;

  /** Returns end offset for the current position, or -1 if
   *  offsets were not indexed. */
  public abstract int endOffset() throws IOException;

  /** Returns the payload at this position, or null if no
   *  payload was indexed. You should not modify anything 
   *  (neither members of the returned BytesRef nor bytes 
   *  in the byte[]). */
  public abstract BytesRef getPayload() throws IOException;

}
