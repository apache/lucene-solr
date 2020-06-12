/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;

/**
 * Cursor into the terms that advances by prefix.
 */
class TermPrefixCursor {

  //Note: this could be a lot more efficient if MemoryPostingsFormat supported ordinal lookup.
  // Maybe that could be added to Lucene.

  // TODO add bloom filter of hashcode of first ~ 6 bytes to avoid lookup into terms dict?

  private static final byte SEPARATOR_CHAR = ConcatenateGraphFilter.SEP_LABEL; // used to be ' '; TODO configurable?
  private static final IntsRef EMPTY_INTSREF = new IntsRef();

  private final TermsEnum termsEnum;
  private final Bits liveDocs;
  private final Map<BytesRef, IntsRef> docIdsCache;

  private BytesRef prefixBuf;//we append to this
  private BytesRefBuilder prefixBufBuilder = new BytesRefBuilder();
  private boolean prefixBufOnLoan;//if true, PB is loaned; needs to be copied
  private PostingsEnum postingsEnum;
  private IntsRef docIds;

  TermPrefixCursor(TermsEnum termsEnum, Bits liveDocs, Map<BytesRef, IntsRef> docIdsCache) {
    this.termsEnum = termsEnum;
    this.liveDocs = liveDocs;
    this.docIdsCache = docIdsCache;
  }

  /** Appends the separator char (if not the first) plus the given word to the prefix buffer,
   * then seeks to it. If the seek fails, false is returned and this cursor
   * can be re-used as if in a new state.  The {@code word} BytesRef is considered temporary,
   * and is not saved within this class. */
  boolean advance(BytesRef word) throws IOException {
    if (prefixBuf == null) { // first advance
      //set prefixBuf to word temporary. When advance() completes, we either null out or copy.
      prefixBuf = word;
      prefixBufOnLoan = true;
      if (seekPrefix()) {//... and we have to
        ensureBufIsACopy();
        return true;
      } else {
        prefixBuf = null;//just to be darned sure 'word' isn't referenced here
        return false;
      }

    } else { // subsequent advance
      //append to existing
      assert !prefixBufOnLoan;

      prefixBufBuilder.append(SEPARATOR_CHAR);
      prefixBufBuilder.append(word);
      prefixBuf = prefixBufBuilder.get();
      if (seekPrefix()) {
        return true;
      } else {
        prefixBuf = null;
        return false;
      }
    }
  }

  private void ensureBufIsACopy() {
    if (!prefixBufOnLoan)
      return;

    prefixBufBuilder.clear();
    prefixBufBuilder.copyBytes(prefixBuf);
    prefixBuf = prefixBufBuilder.get();
    prefixBufOnLoan = false;
  }

  /** Seeks to prefixBuf or the next term that is prefixed by prefixBuf plus the separator char.
   * Sets docIds. **/
  @SuppressWarnings({"fallthrough"})
  private boolean seekPrefix() throws IOException {
    TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(prefixBuf);

    docIds = null;//invalidate
    switch (seekStatus) {
      case END:
        return false;

      case FOUND:
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        docIds = postingsEnumToIntsRef(postingsEnum, liveDocs);
        if (docIds.length > 0) {
          return true;
        }

        //Pretend we didn't find it; go to next term
        docIds = null;
        if (termsEnum.next() == null) { // case END
          return false;
        }
        //fall through to NOT_FOUND

      case NOT_FOUND:
        //termsEnum must start with prefixBuf to continue
        BytesRef teTerm = termsEnum.term();

        if (teTerm.length > prefixBuf.length) {
          for (int i = 0; i < prefixBuf.length; i++) {
            if (prefixBuf.bytes[prefixBuf.offset + i] != teTerm.bytes[teTerm.offset + i])
              return false;
          }
          if (teTerm.bytes[teTerm.offset + prefixBuf.length] != SEPARATOR_CHAR)
            return false;
          return true;
        }
        return false;
    }
    throw new IllegalStateException(seekStatus.toString());
  }

  /** Returns an IntsRef either cached or reading postingsEnum. Not null. */
  private IntsRef postingsEnumToIntsRef(PostingsEnum postingsEnum, Bits liveDocs) throws IOException {
    // (The cache can have empty IntsRefs)

    //lookup prefixBuf in a cache
    if (docIdsCache != null) {
      docIds = docIdsCache.get(prefixBuf);
      if (docIds != null) {
        return docIds;
      }
    }

    //read postingsEnum
    docIds = new IntsRef(termsEnum.docFreq());
    int docId;
    while ((docId = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
      if (liveDocs != null && !liveDocs.get(postingsEnum.docID())) {
        continue;
      }
      docIds.ints[docIds.length++] = docId;
    }
    if (docIds.length == 0)
      docIds = EMPTY_INTSREF;

    //cache
    if (docIdsCache != null) {
      ensureBufIsACopy();
      //clone is shallow; that's okay as the prefix isn't overwritten; it's just appended to
      docIdsCache.put(prefixBuf.clone(), docIds);
    }
    return docIds;
  }

  /** The docIds of the last call to advance, if it returned true. It might be null, but
   * its length won't be 0. Treat as immutable. */
  IntsRef getDocIds() {
    assert docIds == null || docIds.length != 0;
    return docIds;
  }
}
