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

/** Implements flex API (FieldsEnum/TermsEnum) on top of
 *  pre-flex API.  Used only for IndexReader impls outside
 *  Lucene's core.
 *
 *  @deprecated Migrate the external reader to the flex API */
@Deprecated
class LegacyFieldsEnum extends FieldsEnum {
  private final IndexReader r;
  private TermEnum terms;
  private String field;
  private boolean init;

  public LegacyFieldsEnum(IndexReader r) throws IOException {
    this.r = r;
    terms = r.terms();
    init = true;
  }

  @Override
  public String next() throws IOException {

    if (field != null) {
      terms.close();
      // jump to end of the current field:
      terms = r.terms(new Term(field, "\uFFFF"));
      assert terms.term() == null || !terms.term().field.equals(field);
    }
    if (init) {
      init = false;
      if (!terms.next()) {
        return null;
      }
    }
    if (terms.term() != null) {
      String newField = terms.term().field;
      assert field == null || !newField.equals(field);
      field = newField;
      return field;
    } else {
      return null;
    }
  }

  @Override
  public TermsEnum terms() throws IOException {
    return new LegacyTermsEnum(r, field);
  }

  static class LegacyTermsEnum extends TermsEnum {
    private final IndexReader r;
    private final String field;
    private TermEnum terms;
    private BytesRef current;
    private final BytesRef tr = new BytesRef();

    LegacyTermsEnum(IndexReader r, String field) throws IOException {
      this.r = r;
      this.field = field;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return BytesRef.getUTF8SortedAsUTF16Comparator();
    }

    @Override
    public SeekStatus seek(BytesRef text, boolean useCache) throws IOException {
      if (terms != null) {
        terms.close();
      }
      terms = r.terms(new Term(field, text.utf8ToString()));

      final Term t = terms.term();
      if (t == null) {
        current = null;
        return SeekStatus.END;
      } else if (t.field() == field) {
        tr.copy(t.text());
        current = tr;
        if (text.bytesEquals(tr)) {
          return SeekStatus.FOUND;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      } else {
        return SeekStatus.END;
      }
    }

    @Override
    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (terms == null) {
        // first next -- seek to start of field
        terms = r.terms(new Term(field, ""));
        final Term t = terms.term();
        if (t == null || t.field != field) {
          return null;
        } else {
          tr.copy(terms.term().text());
          return current = tr;
        }
      } else if (terms.next()) {
        if (terms.term().field == field) {
          tr.copy(terms.term().text());
          return current = tr;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    @Override
    public BytesRef term() {
      return current;
    }

    @Override
    public int docFreq() {
      return terms.docFreq();
    }

    @Override
    public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((LegacyDocsEnum) reuse).reset(terms.term(), skipDocs);
      } else {
        return (new LegacyDocsEnum(r, field)).reset(terms.term(), skipDocs);
      }
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((LegacyDocsAndPositionsEnum) reuse).reset(terms.term(), skipDocs);
      } else {
        return (new LegacyDocsAndPositionsEnum(r, field)).reset(terms.term(), skipDocs);
      }
    }

    public void close() throws IOException {
      terms.close();
    }
  }

  // Emulates flex on top of legacy API
  private static class LegacyDocsEnum extends DocsEnum {
    private final IndexReader r;
    private final String field;
    private final TermDocs td;

    private Term term;

    private int doc = -1;

    LegacyDocsEnum(IndexReader r, String field) throws IOException {
      this.r = r;
      this.field = field;
      td = r.termDocs();
    }

    public DocsEnum reset(Term term, Bits skipDocs) throws IOException {
      this.term = term;
      td.seek(term);

      if (skipDocs != MultiFields.getDeletedDocs(r)) {
        // An external reader's TermDocs/Positions will
        // silently skip deleted docs, so, we can't allow
        // arbitrary skipDocs here:
        throw new IllegalStateException("external IndexReader requires skipDocs == MultiFields.getDeletedDocs()");
      }

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (td.next()) {
        return doc = td.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (td.skipTo(target)) {
        return doc = td.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return td.freq();
    }

    @Override
    public int docID() {
      return doc;
    }
  }

  // Emulates flex on top of legacy API
  private static class LegacyDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private final IndexReader r;
    private final String field;
    private final TermPositions tp;

    private Term term;

    private int doc = -1;

    LegacyDocsAndPositionsEnum(IndexReader r, String field) throws IOException {
      this.r = r;
      this.field = field;
      tp = r.termPositions();
    }

    public DocsAndPositionsEnum reset(Term term, Bits skipDocs) throws IOException {
      this.term = term;
      tp.seek(term);

      if (skipDocs != MultiFields.getDeletedDocs(r)) {
        // An external reader's TermDocs/Positions will
        // silently skip deleted docs, so, we can't allow
        // arbitrary skipDocs here:
        throw new IllegalStateException("external IndexReader requires skipDocs == MultiFields.getDeletedDocs() skipDocs=" + skipDocs + " MultiFields.getDeletedDocs=" + MultiFields.getDeletedDocs(r) + " r=" + r);
      }

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (tp.next()) {
        return doc = tp.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (tp.skipTo(target)) {
        return doc = tp.doc();
      } else {
        return doc = NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return tp.freq();
    }

    @Override
    public int docID() {
      return doc;
    }

    // NOTE: we don't override bulk-read (docs & freqs) API
    // -- leave it to base class, because TermPositions
    // can't do bulk read

    @Override
    public int nextPosition() throws IOException {
      return tp.nextPosition();
    }

    private BytesRef payload;

    @Override
    public BytesRef getPayload() throws IOException {
      final int len = tp.getPayloadLength();
      if (payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[len];
      } else {
        if (payload.bytes.length < len) {
          payload.grow(len);
        }
      }
      
      payload.bytes = tp.getPayload(payload.bytes, 0);
      payload.length = len;
      return payload;
    }

    @Override
    public boolean hasPayload() {
      return tp.isPayloadAvailable();
    }
  }
}