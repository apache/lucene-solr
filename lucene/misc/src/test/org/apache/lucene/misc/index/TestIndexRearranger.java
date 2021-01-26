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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexRearranger extends LuceneTestCase {
  public void testRearrange() throws Exception {
    Directory inputDir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            inputDir,
            new IndexWriterConfig(null)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setIndexSort(new Sort(new SortField("ord", SortField.Type.INT))));
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("ord", i));
      w.addDocument(doc);
      if (i % 10 == 9) {
        w.commit();
      }
    }
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(10, reader.leaves().size());
    reader.close();
    w.close();

    Directory outputDir = newDirectory();
    IndexRearranger rearranger =
        new IndexRearranger(
            inputDir,
            outputDir,
            new IndexWriterConfig(null)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setIndexSort(new Sort(new SortField("ord", SortField.Type.INT))),
            List.of(new OddDocSelector(), new EvenDocSelector()));
    rearranger.execute();
    reader = DirectoryReader.open(outputDir);
    assertEquals(2, reader.leaves().size());
    LeafReader segment1 = reader.leaves().get(0).reader();
    assertEquals(50, segment1.numDocs());
    NumericDocValues numericDocValues = segment1.getNumericDocValues("ord");
    assertTrue(numericDocValues.advanceExact(0));
    boolean odd = numericDocValues.longValue() % 2 == 1;
    for (int i = 1; i < 50; i++) {
      assertTrue(numericDocValues.advanceExact(i));
      if (odd) {
        assertEquals(1, numericDocValues.longValue() % 2);
      } else {
        assertEquals(0, numericDocValues.longValue() % 2);
      }
    }
    LeafReader segment2 = reader.leaves().get(0).reader();
    assertEquals(50, segment2.numDocs());
    numericDocValues = segment2.getNumericDocValues("ord");
    assertTrue(numericDocValues.advanceExact(0));
    odd = numericDocValues.longValue() % 2 == 1;
    for (int i = 1; i < 50; i++) {
      assertTrue(numericDocValues.advanceExact(i));
      if (odd) {
        assertEquals(1, numericDocValues.longValue() % 2);
      } else {
        assertEquals(0, numericDocValues.longValue() % 2);
      }
    }
    reader.close();
    inputDir.close();
    outputDir.close();
  }

  private class OddDocSelector implements IndexRearranger.DocumentSelector {

    @Override
    public BitSet getFilteredLiveDocs(CodecReader reader) throws IOException {
      FixedBitSet filteredSet = new FixedBitSet(reader.maxDoc());
      Bits liveDocs = reader.getLiveDocs();
      NumericDocValues numericDocValues = reader.getNumericDocValues("ord");
      for (int i = 0; i < reader.maxDoc(); i++) {
        if (liveDocs != null && liveDocs.get(i) == false) {
          continue;
        }
        if (numericDocValues.advanceExact(i) && numericDocValues.longValue() % 2 == 1) {
          filteredSet.set(i);
        }
      }
      return filteredSet;
    }
  }

  private class EvenDocSelector implements IndexRearranger.DocumentSelector {

    @Override
    public BitSet getFilteredLiveDocs(CodecReader reader) throws IOException {
      FixedBitSet filteredSet = new FixedBitSet(reader.maxDoc());
      Bits liveDocs = reader.getLiveDocs();
      NumericDocValues numericDocValues = reader.getNumericDocValues("ord");
      for (int i = 0; i < reader.maxDoc(); i++) {
        if (liveDocs != null && liveDocs.get(i) == false) {
          continue;
        }
        if (numericDocValues.advanceExact(i) && numericDocValues.longValue() % 2 == 0) {
          filteredSet.set(i);
        }
      }
      return filteredSet;
    }
  }
}
