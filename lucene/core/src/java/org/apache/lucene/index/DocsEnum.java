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

/** Iterates through the documents and term freqs.
 *  NOTE: you must first call {@link #nextDoc} before using
 *  any of the per-doc methods. */
public abstract class DocsEnum extends DocIdSetIterator {
  
  /**
   * Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)} if you don't
   * require term frequencies in the returned enum.
   */
  public static final int FLAG_NONE = 0x0;

  /** Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)}
   *  if you require term frequencies in the returned enum. */
  public static final int FLAG_FREQS = 0x1;

  /** Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)}
   * if you require term positions in the returned enum. */
  public static final int FLAG_POSITIONS = 0x3;
  
  /** Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)}
   *  if you require offsets in the returned enum. */
  public static final int FLAG_OFFSETS = 0x7;

  /** Flag to pass to  {@link TermsEnum#docs(Bits,DocsEnum,int)}
   *  if you require payloads in the returned enum. */
  public static final int FLAG_PAYLOADS = 0xB;

  public static final int NO_MORE_POSITIONS = Integer.MAX_VALUE;

  private AttributeSource atts = null;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocsEnum() {
  }

  /**
   * Returns term frequency in the current document, or 1 if the field was
   * indexed with {@link IndexOptions#DOCS}. Do not call this before
   * {@link #nextDoc} is first called, nor after {@link #nextDoc} returns
   * {@link DocIdSetIterator#NO_MORE_DOCS}.
   * 
   * <p>
   * <b>NOTE:</b> if the {@link DocsEnum} was obtain with {@link #FLAG_NONE},
   * the result of this method is undefined.
   */
  public abstract int freq() throws IOException;
  
  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }

  /** Returns the next position.  You should only call this
   *  up to {@link DocsEnum#freq()} times else
   *  the behavior is not defined.  If positions were not
   *  indexed this will return -1; this only happens if
   *  offsets were indexed and you passed needsOffset=true
   *  when pulling the enum.  */
  public abstract int nextPosition() throws IOException;

  public abstract int startPosition() throws IOException;

  public abstract int endPosition() throws IOException;

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
