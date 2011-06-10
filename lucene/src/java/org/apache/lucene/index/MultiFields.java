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
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.ReaderUtil.Gather;  // for javadocs
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg DirectoryReader or {@link
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
   *  fly.  This method will not return null.
   *
   *  <p><b>NOTE</b>: this is a slow way to access postings.
   *  It's better to get the sub-readers (using {@link
   *  Gather}) and iterate through them
   *  yourself. */
  public static Fields getFields(IndexReader r) throws IOException {
    final IndexReader[] subs = r.getSequentialSubReaders();
    if (subs == null) {
      // already an atomic reader
      return r.fields();
    } else if (subs.length == 0) {
      // no fields
      return null;
    } else if (subs.length == 1) {
      return getFields(subs[0]);
    } else {

      Fields currentFields = r.retrieveFields();
      if (currentFields == null) {
      
        final List<Fields> fields = new ArrayList<Fields>();
        final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();

        new ReaderUtil.Gather(r) {
          @Override
          protected void add(int base, IndexReader r) throws IOException {
            final Fields f = r.fields();
            if (f != null) {
              fields.add(f);
              slices.add(new ReaderUtil.Slice(base, r.maxDoc(), fields.size()-1));
            }
          }
        }.run();

        if (fields.size() == 0) {
          return null;
        } else if (fields.size() == 1) {
          currentFields = fields.get(0);
        } else {
          currentFields = new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                         slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY));
        }
        r.storeFields(currentFields);
      }
      return currentFields;
    }
  }

  private static class MultiReaderBits implements Bits {
    private final int[] starts;
    private final IndexReader[] readers;
    private final Bits[] delDocs;

    public MultiReaderBits(int[] starts, IndexReader[] readers) {
      assert readers.length == starts.length-1;
      this.starts = starts;
      this.readers = readers;
      delDocs = new Bits[readers.length];
      for(int i=0;i<readers.length;i++) {
        delDocs[i] = readers[i].getDeletedDocs();
      }
    }
    
    public boolean get(int doc) {
      final int sub = ReaderUtil.subIndex(doc, starts);
      Bits dels = delDocs[sub];
      if (dels == null) {
        // NOTE: this is not sync'd but multiple threads can
        // come through here; I think this is OK -- worst
        // case is more than 1 thread ends up filling in the
        // sub Bits
        dels = readers[sub].getDeletedDocs();
        if (dels == null) {
          return false;
        } else {
          delDocs[sub] = dels;
        }
      }
      return dels.get(doc-starts[sub]);
    }

    public int length() {    
      return starts[starts.length-1];
    }
  }

  public static Bits getDeletedDocs(IndexReader r) {
    Bits result;
    if (r.hasDeletions()) {

      final List<IndexReader> readers = new ArrayList<IndexReader>();
      final List<Integer> starts = new ArrayList<Integer>();

      try {
        final int maxDoc = new ReaderUtil.Gather(r) {
            @Override
            protected void add(int base, IndexReader r) throws IOException {
              // record all delDocs, even if they are null
              readers.add(r);
              starts.add(base);
            }
          }.run();
        starts.add(maxDoc);
      } catch (IOException ioe) {
        // should not happen
        throw new RuntimeException(ioe);
      }

      assert readers.size() > 0;
      if (readers.size() == 1) {
        // Only one actual sub reader -- optimize this case
        result = readers.get(0).getDeletedDocs();
      } else {
        int[] startsArray = new int[starts.size()];
        for(int i=0;i<startsArray.length;i++) {
          startsArray[i] = starts.get(i);
        }
        result = new MultiReaderBits(startsArray, readers.toArray(new IndexReader[readers.size()]));
      }

    } else {
      result = null;
    }

    return result;
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
  public static DocsEnum getTermDocsEnum(IndexReader r, Bits skipDocs, String field, BytesRef term) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      return terms.docs(skipDocs, term, null);
    } else {
      return null;
    }
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  field & term.  This may return null if the term does
   *  not exist or positions were not indexed. */
  public static DocsAndPositionsEnum getTermPositionsEnum(IndexReader r, Bits skipDocs, String field, BytesRef term) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      return terms.docsAndPositions(skipDocs, term, null);
    } else {
      return null;
    }
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
      return new MultiFieldsEnum(fieldsEnums.toArray(FieldsEnum.EMPTY_ARRAY),
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
}

