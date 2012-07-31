package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

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

/**
 * A {@link FilterAtomicReader} that can be used to apply
 * additional checks for tests.
 */
public class AssertingAtomicReader extends FilterAtomicReader {

  public AssertingAtomicReader(AtomicReader in) {
    super(in);
    // check some basic reader sanity
    assert in.maxDoc() >= 0;
    assert in.numDocs() <= in.maxDoc();
    assert in.numDeletedDocs() + in.numDocs() == in.maxDoc();
    assert !in.hasDeletions() || in.numDeletedDocs() > 0 && in.numDocs() < in.maxDoc();
  }

  @Override
  public Fields fields() throws IOException {
    Fields fields = super.fields();
    return fields == null ? null : new AssertingFields(fields);
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    Fields fields = super.getTermVectors(docID);
    return fields == null ? null : new AssertingFields(fields);
  }

  /**
   * Wraps a Fields but with additional asserts
   */
  public static class AssertingFields extends FilterFields {
    public AssertingFields(Fields in) {
      super(in);
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      FieldsEnum fieldsEnum = super.iterator();
      assert fieldsEnum != null;
      return new AssertingFieldsEnum(fieldsEnum);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      return terms == null ? null : new AssertingTerms(terms);
    }
  }
  
  /**
   * Wraps a FieldsEnum but with additional asserts
   */
  public static class AssertingFieldsEnum extends FilterFieldsEnum {
    public AssertingFieldsEnum(FieldsEnum in) {
      super(in);
    }

    @Override
    public Terms terms() throws IOException {
      Terms terms = super.terms();
      return terms == null ? null : new AssertingTerms(terms);
    }
  }
  
  /**
   * Wraps a Terms but with additional asserts
   */
  public static class AssertingTerms extends FilterTerms {
    public AssertingTerms(Terms in) {
      super(in);
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton automaton, BytesRef bytes) throws IOException {
      TermsEnum termsEnum = super.intersect(automaton, bytes);
      assert termsEnum != null;
      return new AssertingTermsEnum(termsEnum);
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      // TODO: should we give this thing a random to be super-evil,
      // and randomly *not* unwrap?
      if (reuse instanceof AssertingTermsEnum) {
        reuse = ((AssertingTermsEnum) reuse).in;
      }
      TermsEnum termsEnum = super.iterator(reuse);
      assert termsEnum != null;
      return new AssertingTermsEnum(termsEnum);
    }
  }
  
  static class AssertingTermsEnum extends FilterTermsEnum {
    private enum State {INITIAL, POSITIONED, UNPOSITIONED};
    private State state = State.INITIAL;

    public AssertingTermsEnum(TermsEnum in) {
      super(in);
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      assert state == State.POSITIONED: "docs(...) called on unpositioned TermsEnum";

      // TODO: should we give this thing a random to be super-evil,
      // and randomly *not* unwrap?
      if (reuse instanceof AssertingDocsEnum) {
        reuse = ((AssertingDocsEnum) reuse).in;
      }
      DocsEnum docs = super.docs(liveDocs, reuse, flags);
      return docs == null ? null : new AssertingDocsEnum(docs);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      assert state == State.POSITIONED: "docsAndPositions(...) called on unpositioned TermsEnum";

      // TODO: should we give this thing a random to be super-evil,
      // and randomly *not* unwrap?
      if (reuse instanceof AssertingDocsAndPositionsEnum) {
        reuse = ((AssertingDocsAndPositionsEnum) reuse).in;
      }
      DocsAndPositionsEnum docs = super.docsAndPositions(liveDocs, reuse, flags);
      return docs == null ? null : new AssertingDocsAndPositionsEnum(docs);
    }

    // TODO: we should separately track if we are 'at the end' ?
    // someone should not call next() after it returns null!!!!
    @Override
    public BytesRef next() throws IOException {
      assert state == State.INITIAL || state == State.POSITIONED: "next() called on unpositioned TermsEnum";
      BytesRef result = super.next();
      if (result == null) {
        state = State.UNPOSITIONED;
      } else {
        state = State.POSITIONED;
      }
      return result;
    }

    @Override
    public long ord() throws IOException {
      assert state == State.POSITIONED : "ord() called on unpositioned TermsEnum";
      return super.ord();
    }

    @Override
    public int docFreq() throws IOException {
      assert state == State.POSITIONED : "docFreq() called on unpositioned TermsEnum";
      return super.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      assert state == State.POSITIONED : "totalTermFreq() called on unpositioned TermsEnum";
      return super.totalTermFreq();
    }

    @Override
    public BytesRef term() throws IOException {
      assert state == State.POSITIONED : "term() called on unpositioned TermsEnum";
      return super.term();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      super.seekExact(ord);
      state = State.POSITIONED;
    }

    @Override
    public SeekStatus seekCeil(BytesRef term, boolean useCache) throws IOException {
      SeekStatus result = super.seekCeil(term, useCache);
      if (result == SeekStatus.END) {
        state = State.UNPOSITIONED;
      } else {
        state = State.POSITIONED;
      }
      return result;
    }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
      if (super.seekExact(text, useCache)) {
        state = State.POSITIONED;
        return true;
      } else {
        state = State.UNPOSITIONED;
        return false;
      }
    }

    @Override
    public TermState termState() throws IOException {
      assert state == State.POSITIONED : "termState() called on unpositioned TermsEnum";
      return super.termState();
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      super.seekExact(term, state);
      this.state = State.POSITIONED;
    }
  }
  
  static enum DocsEnumState { START, ITERATING, FINISHED };
  static class AssertingDocsEnum extends FilterDocsEnum {
    private DocsEnumState state = DocsEnumState.START;
    
    public AssertingDocsEnum(DocsEnum in) {
      super(in);
      int docid = in.docID();
      assert docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS : "invalid initial doc id: " + docid;
    }

    @Override
    public int nextDoc() throws IOException {
      assert state != DocsEnumState.FINISHED : "nextDoc() called after NO_MORE_DOCS";
      int nextDoc = super.nextDoc();
      assert nextDoc >= 0 : "invalid doc id: " + nextDoc;
      if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
        state = DocsEnumState.FINISHED;
      } else {
        state = DocsEnumState.ITERATING;
      }
      return nextDoc;
    }

    @Override
    public int advance(int target) throws IOException {
      assert state != DocsEnumState.FINISHED : "advance() called after NO_MORE_DOCS";
      int advanced = super.advance(target);
      assert advanced >= 0 : "invalid doc id: " + advanced;
      assert advanced >= target : "backwards advance from: " + target + " to: " + advanced;
      if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
        state = DocsEnumState.FINISHED;
      } else {
        state = DocsEnumState.ITERATING;
      }
      return advanced;
    }

    // NOTE: We don't assert anything for docId(). Specifically DocsEnum javadocs
    // are ambiguous with DocIdSetIterator here, DocIdSetIterator says its ok
    // to call this method before nextDoc(), just that it must be -1 or NO_MORE_DOCS!

    @Override
    public int freq() throws IOException {
      assert state != DocsEnumState.START : "freq() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "freq() called after NO_MORE_DOCS";
      int freq = super.freq();
      assert freq > 0;
      return freq;
    }
  }
  
  static class AssertingDocsAndPositionsEnum extends FilterDocsAndPositionsEnum {
    private DocsEnumState state = DocsEnumState.START;
    private int positionMax = 0;
    private int positionCount = 0;

    public AssertingDocsAndPositionsEnum(DocsAndPositionsEnum in) {
      super(in);
      int docid = in.docID();
      assert docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS : "invalid initial doc id: " + docid;
    }

    @Override
    public int nextDoc() throws IOException {
      assert state != DocsEnumState.FINISHED : "nextDoc() called after NO_MORE_DOCS";
      int nextDoc = super.nextDoc();
      assert nextDoc >= 0 : "invalid doc id: " + nextDoc;
      positionCount = 0;
      if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
        state = DocsEnumState.FINISHED;
        positionMax = 0;
      } else {
        state = DocsEnumState.ITERATING;
        positionMax = super.freq();
      }
      return nextDoc;
    }

    @Override
    public int advance(int target) throws IOException {
      assert state != DocsEnumState.FINISHED : "advance() called after NO_MORE_DOCS";
      int advanced = super.advance(target);
      assert advanced >= 0 : "invalid doc id: " + advanced;
      assert advanced >= target : "backwards advance from: " + target + " to: " + advanced;
      positionCount = 0;
      if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
        state = DocsEnumState.FINISHED;
        positionMax = 0;
      } else {
        state = DocsEnumState.ITERATING;
        positionMax = super.freq();
      }
      return advanced;
    }

    @Override
    public int freq() throws IOException {
      assert state != DocsEnumState.START : "freq() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "freq() called after NO_MORE_DOCS";
      int freq = super.freq();
      assert freq > 0;
      return freq;
    }

    @Override
    public int nextPosition() throws IOException {
      assert state != DocsEnumState.START : "nextPosition() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "nextPosition() called after NO_MORE_DOCS";
      assert positionCount < positionMax : "nextPosition() called more than freq() times!";
      int position = super.nextPosition();
      assert position >= 0 || position == -1 : "invalid position: " + position;
      positionCount++;
      return position;
    }

    @Override
    public int startOffset() throws IOException {
      assert state != DocsEnumState.START : "startOffset() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "startOffset() called after NO_MORE_DOCS";
      assert positionCount > 0 : "startOffset() called before nextPosition()!";
      return super.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      assert state != DocsEnumState.START : "endOffset() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "endOffset() called after NO_MORE_DOCS";
      assert positionCount > 0 : "endOffset() called before nextPosition()!";
      return super.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      assert state != DocsEnumState.START : "getPayload() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "getPayload() called after NO_MORE_DOCS";
      assert positionCount > 0 : "getPayload() called before nextPosition()!";
      return super.getPayload();
    }

    @Override
    public boolean hasPayload() {
      assert state != DocsEnumState.START : "hasPayload() called before nextDoc()/advance()";
      assert state != DocsEnumState.FINISHED : "hasPayload() called after NO_MORE_DOCS";
      assert positionCount > 0 : "hasPayload() called before nextPosition()!";
      return super.hasPayload();
    }
  }

  // this is the same hack as FCInvisible
  @Override
  public Object getCoreCacheKey() {
    return cacheKey;
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return cacheKey;
  }
  
  private final Object cacheKey = new Object();
}