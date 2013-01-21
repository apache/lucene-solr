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

// nocommit move this back to test-framework!!!
public class MultiDocValues {
  
  // moved to src/java so SlowWrapper can use it... uggggggh
  public static NumericDocValues getNormValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader().getNormValues(field);
    }
    FieldInfo fi = MultiFields.getMergedFieldInfos(r).fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      return null;
    }
    boolean anyReal = false;
    for(AtomicReaderContext ctx : leaves) {
      NumericDocValues norms = ctx.reader().getNormValues(field);

      if (norms != null) {
        anyReal = true;
      }
    }

    assert anyReal;

    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        int subIndex = ReaderUtil.subIndex(docID, leaves);
        NumericDocValues norms;
        try {
          norms = leaves.get(subIndex).reader().getNormValues(field);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        if (norms == null) {
          return 0;
        } else {
          return norms.get(docID - leaves.get(subIndex).docBase);
        }
      }
    };
  }

  public static NumericDocValues getNumericValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader().getNumericDocValues(field);
    }
    boolean anyReal = false;
    for(AtomicReaderContext ctx : leaves) {
      NumericDocValues values = ctx.reader().getNumericDocValues(field);

      if (values != null) {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          NumericDocValues values;
          try {
            values = leaves.get(subIndex).reader().getNumericDocValues(field);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (values == null) {
            return 0;
          } else {
            return values.get(docID - leaves.get(subIndex).docBase);
          }
        }
      };
    }
  }

  public static BinaryDocValues getBinaryValues(final IndexReader r, final String field) throws IOException {
    final List<AtomicReaderContext> leaves = r.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader().getBinaryDocValues(field);
    }
    boolean anyReal = false;

    for(AtomicReaderContext ctx : leaves) {
      BinaryDocValues values = ctx.reader().getBinaryDocValues(field);

      if (values != null) {
        anyReal = true;
      }
    }

    if (!anyReal) {
      return null;
    } else {

      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          BinaryDocValues values;
          try {
            values = leaves.get(subIndex).reader().getBinaryDocValues(field);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (values != null) {
            values.get(docID - leaves.get(subIndex).docBase, result);
          } else {
            result.length = 0;
            result.bytes = BinaryDocValues.MISSING;
          }
        }
      };
    }
  }
  
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
