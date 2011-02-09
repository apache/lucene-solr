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
import java.util.Comparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;

/**
 * Access to the terms in a specific field.  See {@link Fields}.
 * @lucene.experimental
 */

public abstract class Terms {

  // Privately cache a TermsEnum per-thread for looking up
  // docFreq and getting a private DocsEnum
  private final CloseableThreadLocal<TermsEnum> threadEnums = new CloseableThreadLocal<TermsEnum>();

  /** Returns an iterator that will step through all
   *  terms. This method will not return null.*/
  public abstract TermsEnum iterator() throws IOException;
  
  /** Return the BytesRef Comparator used to sort terms
   *  provided by the iterator.  This method may return null
   *  if there are no terms.  This method may be invoked
   *  many times; it's best to cache a single instance &
   *  reuse it. */
  public abstract Comparator<BytesRef> getComparator() throws IOException;

  /** Returns the number of documents containing the
   *  specified term text.  Returns 0 if the term does not
   *  exist. */
  public int docFreq(BytesRef text) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    if (termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return termsEnum.docFreq();
    } else {
      return 0;
    }
  }

  /** Returns the number of documents containing the
   *  specified term text.  Returns 0 if the term does not
   *  exist. */
  public long totalTermFreq(BytesRef text) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    if (termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return termsEnum.totalTermFreq();
    } else {
      return 0;
    }
  }

  /** Get {@link DocsEnum} for the specified term.  This
   *  method may return null if the term does not exist. */
  public DocsEnum docs(Bits skipDocs, BytesRef text, DocsEnum reuse) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    if (termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return termsEnum.docs(skipDocs, reuse);
    } else {
      return null;
    }
  }

  /** Get {@link DocsEnum} for the specified term.  This
   *  method will may return null if the term does not
   *  exists, or positions were not indexed. */ 
  public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, BytesRef text, DocsAndPositionsEnum reuse) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    if (termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
      return termsEnum.docsAndPositions(skipDocs, reuse);
    } else {
      return null;
    }
  }

  /**
   * Expert: Get {@link DocsEnum} for the specified {@link TermState}.
   * This method may return <code>null</code> if the term does not exist.
   * 
   * @see TermsEnum#termState()
   * @see TermsEnum#seek(BytesRef, TermState) */
  public DocsEnum docs(Bits skipDocs, BytesRef term, TermState termState, DocsEnum reuse) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    termsEnum.seek(term, termState);
    return termsEnum.docs(skipDocs, reuse);
  }

  /**
   * Get {@link DocsEnum} for the specified {@link TermState}. This
   * method will may return <code>null</code> if the term does not exists, or positions were
   * not indexed.
   * 
   * @see TermsEnum#termState()
   * @see TermsEnum#seek(BytesRef, TermState) */
  public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, BytesRef term, TermState termState, DocsAndPositionsEnum reuse) throws IOException {
    final TermsEnum termsEnum = getThreadTermsEnum();
    termsEnum.seek(term, termState);
    return termsEnum.docsAndPositions(skipDocs, reuse);
  }

  public long getUniqueTermCount() throws IOException {
    throw new UnsupportedOperationException("this reader does not implement getUniqueTermCount()");
  }

  /** Returns the sum of {@link TermsEnum#totalTermFreq} for
   *  all terms in this field, or -1 if this measure isn't
   *  stored by the codec (or if this fields omits term freq
   *  and positions).  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract long getSumTotalTermFreq() throws IOException;

  /**
   * Returns a thread-private {@link TermsEnum} instance. Obtaining
   * {@link TermsEnum} from this method might be more efficient than using
   * {@link #iterator()} directly since this method doesn't necessarily create a
   * new {@link TermsEnum} instance.
   * <p>
   * NOTE: {@link TermsEnum} instances obtained from this method must not be
   * shared across threads. The enum should only be used within a local context
   * where other threads can't access it.
   * 
   * @return a thread-private {@link TermsEnum} instance
   * @throws IOException
   *           if an IOException occurs
   * @lucene.internal
   */
  public TermsEnum getThreadTermsEnum() throws IOException {
    TermsEnum termsEnum = threadEnums.get();
    if (termsEnum == null) {
      termsEnum = iterator();
      threadEnums.set(termsEnum);
    }
    return termsEnum;
  }

  // subclass must close when done:
  protected void close() {
    threadEnums.close();
  }
  public final static Terms[] EMPTY_ARRAY = new Terms[0];
}
