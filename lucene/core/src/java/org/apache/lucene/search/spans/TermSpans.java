package org.apache.lucene.search.spans;
/**
 * Copyright 2005 The Apache Software Foundation
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


import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.Collection;

/**
 * Expert:
 * Public for extension only
 */
public class TermSpans extends Spans {
  protected final DocsAndPositionsEnum postings;
  protected final Term term;
  protected int doc;
  protected int freq;
  protected int count;
  protected int position;

  public TermSpans(DocsAndPositionsEnum postings, Term term) throws IOException {
    this.postings = postings;
    this.term = term;
    doc = -1;
  }

  // only for EmptyTermSpans (below)
  TermSpans() {
    term = null;
    postings = null;
  }

  @Override
  public boolean next() throws IOException {
    if (count == freq) {
      if (postings == null) {
        return false;
      }
      doc = postings.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        return false;
      }
      freq = postings.freq();
      count = 0;
    }
    position = postings.nextPosition();
    count++;
    return true;
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    doc = postings.advance(target);
    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
      return false;
    }

    freq = postings.freq();
    count = 0;
    position = postings.nextPosition();
    count++;

    return true;
  }

  @Override
  public int doc() {
    return doc;
  }

  @Override
  public int start() {
    return position;
  }

  @Override
  public int end() {
    return position + 1;
  }

  // TODO: Remove warning after API has been finalized
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    final BytesRef payload = postings.getPayload();
    final byte[] bytes;
    if (payload != null) {
      bytes = new byte[payload.length];
      System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
    } else {
      bytes = null;
    }
    return Collections.singletonList(bytes);
  }

  // TODO: Remove warning after API has been finalized
  @Override
  public boolean isPayloadAvailable() {
    return postings.hasPayload();
  }

  @Override
  public String toString() {
    return "spans(" + term.toString() + ")@" +
            (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
  }

  public DocsAndPositionsEnum getPostings() {
    return postings;
  }

  private static final class EmptyTermSpans extends TermSpans {

    @Override
    public boolean next() {
      return false;
    }

    @Override
    public boolean skipTo(int target) {
      return false;
    }

    @Override
    public int doc() {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    
    @Override
    public int start() {
      return -1;
    }

    @Override
    public int end() {
      return -1;
    }

    @Override
    public Collection<byte[]> getPayload() {
      return null;
    }

    @Override
    public boolean isPayloadAvailable() {
      return false;
    }
  }

  public static final TermSpans EMPTY_TERM_SPANS = new EmptyTermSpans();
}
