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
import java.util.Objects;

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits; // javadocs
import org.apache.lucene.util.BytesRef;

/** 
 * Also iterates through positions. 
 * @deprecated Use {@link PostingsEnum} instead.
 */
@Deprecated
public abstract class DocsAndPositionsEnum extends DocsEnum {
  
  /** Flag to pass to {@link TermsEnum#docsAndPositions(Bits,DocsAndPositionsEnum,int)}
   *  if you require offsets in the returned enum. */
  public static final int FLAG_OFFSETS = 0x1;

  /** Flag to pass to  {@link TermsEnum#docsAndPositions(Bits,DocsAndPositionsEnum,int)}
   *  if you require payloads in the returned enum. */
  public static final int FLAG_PAYLOADS = 0x2;
  
  /**
   * Codec implementations should check for this flag,
   * and return null when positions are requested but not present.
   * @deprecated only for internal use.
   */
  @Deprecated
  public static final short OLD_NULL_SEMANTICS = 1 << 14;

  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected DocsAndPositionsEnum() {
  }

  /** Returns the next position.  You should only call this
   *  up to {@link DocsEnum#freq()} times else
   *  the behavior is not defined.  If positions were not
   *  indexed this will return -1; this only happens if
   *  offsets were indexed and you passed needsOffset=true
   *  when pulling the enum.  */
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
  
  /** 
   * Wraps a PostingsEnum with a legacy DocsAndPositionsEnum.
   */
  static DocsAndPositionsEnum wrap(final PostingsEnum postings) {
    return new DocsAndPositionsEnumWrapper(postings);
  }
  
  /**
   * Unwrap a legacy DocsAndPositionsEnum and return the actual PostingsEnum.
   * if {@code docs} is null, this returns null for convenience
   */
  static PostingsEnum unwrap(final DocsEnum docs) {
    if (docs instanceof DocsAndPositionsEnumWrapper) {
      return ((DocsAndPositionsEnumWrapper)docs).in;
    } else if (docs == null) {
      return null; // e.g. user is not reusing
    } else {
      throw new AssertionError();
    }
  }
  
  static class DocsAndPositionsEnumWrapper extends DocsAndPositionsEnum {
    final PostingsEnum in;
    
    DocsAndPositionsEnumWrapper(PostingsEnum in) {
      this.in = Objects.requireNonNull(in);
    }

    @Override
    public int nextPosition() throws IOException {
      return in.nextPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return in.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return in.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return in.getPayload();
    }

    @Override
    public int freq() throws IOException {
      return in.freq();
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }
}

