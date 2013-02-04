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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * A wrapper for CompositeIndexReader providing access to DocValues.
 * 
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-AtomicReader,
 * instead of using this class.
 * 
 * <p><b>NOTE</b>: This is very costly.
 *
 * @lucene.experimental
 * @lucene.internal
 */
// nocommit move this back to test-framework!!!
public class MultiDocValues {
  
  /** No instantiation */
  private MultiDocValues() {}
  
  /** Returns a NumericDocValues for a reader's norms (potentially merging on-the-fly).
   * <p>
   * This is a slow way to access normalization values. Instead, access them per-segment
   * with {@link AtomicReader#getNormValues(String)}
   * </p> 
   */
  public static NumericDocValues getNormValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNormValues(field);
    }
    FieldInfo fi = MultiFields.getMergedFieldInfos(r).fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      return null;
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNormValues(field);
      if (v == null) {
        v = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    assert anyReal;

    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        int subIndex = ReaderUtil.subIndex(docID, starts);
        return values[subIndex].get(docID - starts[subIndex]);
      }
    };
  }

  /** Returns a NumericDocValues for a reader's docvalues (potentially merging on-the-fly) 
   * <p>
   * This is a slow way to access numeric values. Instead, access them per-segment
   * with {@link AtomicReader#getNumericDocValues(String)}
   * </p> 
   * */
  public static NumericDocValues getNumericValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getNumericDocValues(field);
    }

    boolean anyReal = false;
    final NumericDocValues[] values = new NumericDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      NumericDocValues v = context.reader().getNumericDocValues(field);
      if (v == null) {
        v = NumericDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          return values[subIndex].get(docID - starts[subIndex]);
        }
      };
    }
  }

  /** Returns a BinaryDocValues for a reader's docvalues (potentially merging on-the-fly)
   * <p>
   * This is a slow way to access binary values. Instead, access them per-segment
   * with {@link AtomicReader#getBinaryDocValues(String)}
   * </p>  
   */
  public static BinaryDocValues getBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getBinaryDocValues(field);
    }
    
    boolean anyReal = false;
    final BinaryDocValues[] values = new BinaryDocValues[size];
    final int[] starts = new int[size+1];
    for (int i = 0; i < size; i++) {
      AtomicReaderContext context = leaves.get(i);
      BinaryDocValues v = context.reader().getBinaryDocValues(field);
      if (v == null) {
        v = BinaryDocValues.EMPTY;
      } else {
        anyReal = true;
      }
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = r.maxDoc();
    
    if (!anyReal) {
      return null;
    } else {
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          int subIndex = ReaderUtil.subIndex(docID, starts);
          values[subIndex].get(docID - starts[subIndex], result);
        }
      };
    }
  }
  
  /** Returns a SortedDocValues for a reader's docvalues (potentially doing extremely slow things).
   * <p>
   * This is an extremely slow way to access sorted values. Instead, access them per-segment
   * with {@link AtomicReader#getSortedDocValues(String)}
   * </p>  
   */
  public static SortedDocValues getSortedValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader().getSortedDocValues(field);
    }
    boolean anyReal = false;

    for(AtomicReaderContext ctx : leaves) {
      SortedDocValues values = ctx.reader().getSortedDocValues(field);

      if (values != null) {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {
      // its called slow-wrapper for a reason right?
      final Directory scratch = new RAMDirectory();
      IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_50, null);
      config.setCodec(Codec.forName("SimpleText"));
      IndexWriter writer = new IndexWriter(scratch, config);
      List<AtomicReader> newLeaves = new ArrayList<AtomicReader>();
      // fake up fieldinfos
      FieldInfo fi = new FieldInfo(field, false, 0, false, false, false, null, DocValuesType.SORTED, null, null);
      final FieldInfos fis = new FieldInfos(new FieldInfo[] { fi });
      for (AtomicReaderContext ctx : leaves) {
        final AtomicReader a = ctx.reader();
        newLeaves.add(new FilterAtomicReader(a) {
          @Override
          public Bits getLiveDocs() {
            return null; // lie
          }
          @Override
          public int numDocs() {
            return maxDoc(); // lie
          }
          @Override
          public boolean hasDeletions() {
            return false; // lie
          }
          @Override
          public FieldInfos getFieldInfos() {
            return fis;
          }
          @Override
          public Fields getTermVectors(int docID) throws IOException {
            return null; // lie
          }
          @Override
          public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            // lie
          }
          @Override
          public Fields fields() throws IOException {
            return null; // lie
          }
        });
      }
      writer.addIndexes(newLeaves.toArray(new AtomicReader[0]));
      writer.close();
      final IndexReader newR = DirectoryReader.open(scratch);
      assert newR.leaves().size() == 1;
      r.addReaderClosedListener(new ReaderClosedListener() {
        @Override
        public void onClose(IndexReader reader) {
          IOUtils.closeWhileHandlingException(newR, scratch);
        }
      });
      return newR.leaves().get(0).reader().getSortedDocValues(field);
    }
  }
}
