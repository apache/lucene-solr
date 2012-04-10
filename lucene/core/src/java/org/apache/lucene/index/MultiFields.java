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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MultiBits;
import org.apache.lucene.util.ReaderUtil.Gather;  // for javadocs
import org.apache.lucene.util.ReaderUtil;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg {@link DirectoryReader} or {@link
 * MultiReader}).
 *
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using {@link
 * ReaderUtil#gatherSubReaders} and then operate per-reader,
 * instead of using this class.
 *
 * @lucene.experimental
 */

public final class MultiFields extends Fields {
  private final Fields[] subs;
  private final ReaderUtil.Slice[] subSlices;
  private final Map<String,Terms> terms = new ConcurrentHashMap<String,Terms>();

  /** Returns a single {@link Fields} instance for this
   *  reader, merging fields/terms/docs/positions on the
   *  fly.  This method will return null if the reader 
   *  has no postings.
   *
   *  <p><b>NOTE</b>: this is a slow way to access postings.
   *  It's better to get the sub-readers (using {@link
   *  Gather}) and iterate through them
   *  yourself. */
  public static Fields getFields(IndexReader r) throws IOException {
    if (r instanceof AtomicReader) {
      // already an atomic reader
      return ((AtomicReader) r).fields();
    }
    assert r instanceof CompositeReader;
    final IndexReader[] subs = ((CompositeReader) r).getSequentialSubReaders();
    if (subs.length == 0) {
      // no fields
      return null;
    } else {
      final List<Fields> fields = new ArrayList<Fields>();
      final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();

      new ReaderUtil.Gather(r) {
        @Override
        protected void add(int base, AtomicReader r) throws IOException {
          final Fields f = r.fields();
          if (f != null) {
            fields.add(f);
            slices.add(new ReaderUtil.Slice(base, r.maxDoc(), fields.size()-1));
          }
        }
      }.run();

      if (fields.isEmpty()) {
        return null;
      } else if (fields.size() == 1) {
        return fields.get(0);
      } else {
        return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                       slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
      }
    }
  }

  public static Bits getLiveDocs(IndexReader r) {
    if (r.hasDeletions()) {
      final List<Bits> liveDocs = new ArrayList<Bits>();
      final List<Integer> starts = new ArrayList<Integer>();

      try {
        final int maxDoc = new ReaderUtil.Gather(r) {
            @Override
            protected void add(int base, AtomicReader r) throws IOException {
              // record all liveDocs, even if they are null
              liveDocs.add(r.getLiveDocs());
              starts.add(base);
            }
          }.run();
        starts.add(maxDoc);
      } catch (IOException ioe) {
        // should not happen
        throw new RuntimeException(ioe);
      }

      assert liveDocs.size() > 0;
      if (liveDocs.size() == 1) {
        // Only one actual sub reader -- optimize this case
        return liveDocs.get(0);
      } else {
        return new MultiBits(liveDocs, starts, true);
      }
    } else {
      return null;
    }
  }

  /**  This method may return null if the field does not exist.*/
  public static Terms getTerms(IndexReader r, String field) throws IOException {
    final Fields fields = getFields(r);
    if (fields == null) {
      return null;
    } else {
      return fields.terms(field);
    }
  }
  
  /** Returns {@link DocsEnum} for the specified field &
   *  term.  This may return null if the term does not
   *  exist. */
  public static DocsEnum getTermDocsEnum(IndexReader r, Bits liveDocs, String field, BytesRef term, boolean needsFreqs) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator(null);
      if (termsEnum.seekExact(term, true)) {
        return termsEnum.docs(liveDocs, null, needsFreqs);
      }
    }
    return null;
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  field & term.  This may return null if the term does
   *  not exist or positions were not indexed. */
  public static DocsAndPositionsEnum getTermPositionsEnum(IndexReader r, Bits liveDocs, String field, BytesRef term, boolean needsOffsets) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator(null);
      if (termsEnum.seekExact(term, true)) {
        return termsEnum.docsAndPositions(liveDocs, null, needsOffsets);
      }
    }
    return null;
  }

  public MultiFields(Fields[] subs, ReaderUtil.Slice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
  }

  @Override
  public FieldsEnum iterator() throws IOException {

    final List<FieldsEnum> fieldsEnums = new ArrayList<FieldsEnum>();
    final List<ReaderUtil.Slice> fieldsSlices = new ArrayList<ReaderUtil.Slice>();
    for(int i=0;i<subs.length;i++) {
      fieldsEnums.add(subs[i].iterator());
      fieldsSlices.add(subSlices[i]);
    }
    if (fieldsEnums.size() == 0) {
      return FieldsEnum.EMPTY;
    } else {
      return new MultiFieldsEnum(this,
                                 fieldsEnums.toArray(FieldsEnum.EMPTY_ARRAY),
                                 fieldsSlices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
    }
  }

  @Override
  public Terms terms(String field) throws IOException {

    Terms result = terms.get(field);
    if (result != null)
      return result;


    // Lazy init: first time this field is requested, we
    // create & add to terms:
    final List<Terms> subs2 = new ArrayList<Terms>();
    final List<ReaderUtil.Slice> slices2 = new ArrayList<ReaderUtil.Slice>();

    // Gather all sub-readers that share this field
    for(int i=0;i<subs.length;i++) {
      final Terms terms = subs[i].terms(field);
      if (terms != null) {
        subs2.add(terms);
        slices2.add(subSlices[i]);
      }
    }
    if (subs2.size() == 0) {
      result = null;
      // don't cache this case with an unbounded cache, since the number of fields that don't exist
      // is unbounded.
    } else {
      result = new MultiTerms(subs2.toArray(Terms.EMPTY_ARRAY),
          slices2.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
      terms.put(field, result);
    }

    return result;
  }

  @Override
  public int size() {
    return -1;
  }

  public static long totalTermFreq(IndexReader r, String field, BytesRef text) throws IOException {
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator(null);
      if (termsEnum.seekExact(text, true)) {
        return termsEnum.totalTermFreq();
      }
    }
    return 0;
  }

  /** Call this to get the (merged) FieldInfos for a
   *  composite reader */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final List<AtomicReader> subReaders = new ArrayList<AtomicReader>();
    ReaderUtil.gatherSubReaders(subReaders, reader);
    final FieldInfos fieldInfos = new FieldInfos();
    for(AtomicReader subReader : subReaders) {
      fieldInfos.add(subReader.getFieldInfos());
    }
    return fieldInfos;
  }

  public static Collection<String> getIndexedFields(IndexReader reader) {
    final Collection<String> fields = new HashSet<String>();
    for(FieldInfo fieldInfo : getMergedFieldInfos(reader)) {
      if (fieldInfo.isIndexed) {
        fields.add(fieldInfo.name);
      }
    }
    return fields;
  }
}

