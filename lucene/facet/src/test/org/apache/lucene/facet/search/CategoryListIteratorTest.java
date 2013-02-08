package org.apache.lucene.facet.search;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.encoding.DGapIntEncoder;
import org.apache.lucene.facet.encoding.IntEncoder;
import org.apache.lucene.facet.encoding.SortingIntEncoder;
import org.apache.lucene.facet.encoding.UniqueValuesIntEncoder;
import org.apache.lucene.facet.encoding.VInt8IntEncoder;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.junit.Test;

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

public class CategoryListIteratorTest extends FacetTestCase {

  static final IntsRef[] data = new IntsRef[] {
    new IntsRef(new int[] { 1, 2 }, 0, 2), 
    new IntsRef(new int[] { 3, 4 }, 0, 2),
    new IntsRef(new int[] { 1, 3 }, 0, 2),
    new IntsRef(new int[] { 1, 2, 3, 4 }, 0, 4)
  };

  @Test
  public void test() throws Exception {
    Directory dir = newDirectory();
    final IntEncoder encoder = randomCategoryListParams().createEncoder();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)).setMergePolicy(newLogMergePolicy()));
    BytesRef buf = new BytesRef();
    for (int i = 0; i < data.length; i++) {
      Document doc = new Document();
      encoder.encode(IntsRef.deepCopyOf(data[i]), buf);
      doc.add(new BinaryDocValuesField("f", buf));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    int totalCategories = 0;
    IntsRef ordinals = new IntsRef();
    CategoryListIterator cli = new DocValuesCategoryListIterator("f", encoder.createMatchingDecoder());
    for (AtomicReaderContext context : reader.leaves()) {
      assertTrue("failed to initalize iterator", cli.setNextReader(context));
      int maxDoc = context.reader().maxDoc();
      int dataIdx = context.docBase;
      for (int doc = 0; doc < maxDoc; doc++, dataIdx++) {
        Set<Integer> values = new HashSet<Integer>();
        for (int j = 0; j < data[dataIdx].length; j++) {
          values.add(data[dataIdx].ints[j]);
        }
        cli.getOrdinals(doc, ordinals);
        assertTrue("no ordinals for document " + doc, ordinals.length > 0);
        for (int j = 0; j < ordinals.length; j++) {
          assertTrue("expected category not found: " + ordinals.ints[j], values.contains(ordinals.ints[j]));
        }
        totalCategories += ordinals.length;
      }
    }
    assertEquals("Missing categories!", 10, totalCategories);
    reader.close();
    dir.close();
  }

  @Test
  public void testEmptyDocuments() throws Exception {
    Directory dir = newDirectory();
    final IntEncoder encoder = new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEncoder())));
    // NOTE: test is wired to LogMP... because test relies on certain docids having payloads
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < data.length; i++) {
      Document doc = new Document();
      if (i == 0) {
        BytesRef buf = new BytesRef();
        encoder.encode(IntsRef.deepCopyOf(data[i]), buf );
        doc.add(new BinaryDocValuesField("f", buf));
      } else {
        doc.add(new BinaryDocValuesField("f", new BytesRef()));
      }
      writer.addDocument(doc);
      writer.commit();
    }

    IndexReader reader = writer.getReader();
    writer.close();

    int totalCategories = 0;
    IntsRef ordinals = new IntsRef();
    CategoryListIterator cli = new DocValuesCategoryListIterator("f", encoder.createMatchingDecoder());
    for (AtomicReaderContext context : reader.leaves()) {
      assertTrue("failed to initalize iterator", cli.setNextReader(context));
      int maxDoc = context.reader().maxDoc();
      int dataIdx = context.docBase;
      for (int doc = 0; doc < maxDoc; doc++, dataIdx++) {
        Set<Integer> values = new HashSet<Integer>();
        for (int j = 0; j < data[dataIdx].length; j++) {
          values.add(data[dataIdx].ints[j]);
        }
        cli.getOrdinals(doc, ordinals);
        if (dataIdx == 0) {
          assertTrue("document 0 must have ordinals", ordinals.length > 0);
          for (int j = 0; j < ordinals.length; j++) {
            assertTrue("expected category not found: " + ordinals.ints[j], values.contains(ordinals.ints[j]));
          }
          totalCategories += ordinals.length;
        } else {
          assertTrue("only document 0 should have ordinals", ordinals.length == 0);
        }
      }
    }
    assertEquals("Wrong number of total categories!", 2, totalCategories);

    reader.close();
    dir.close();
  }

}
