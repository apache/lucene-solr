package org.apache.lucene.facet.search;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.encoding.DGapIntEncoder;
import org.apache.lucene.util.encoding.IntEncoder;
import org.apache.lucene.util.encoding.SortingIntEncoder;
import org.apache.lucene.util.encoding.UniqueValuesIntEncoder;
import org.apache.lucene.util.encoding.VInt8IntEncoder;
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

public class CategoryListIteratorTest extends LuceneTestCase {

  private static final class DataTokenStream extends TokenStream {

    private final PayloadAttribute payload = addAttribute(PayloadAttribute.class);
    private final BytesRef buf;
    private final IntEncoder encoder;
    private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
    
    private int idx;
    private boolean exhausted = false;

    public DataTokenStream(String text, IntEncoder encoder) {
      this.encoder = encoder;
      term.setEmpty().append(text);
      buf = new BytesRef();
      payload.setPayload(buf);
    }

    public void setIdx(int idx) {
      this.idx = idx;
      exhausted = false;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (exhausted) {
        return false;
      }

      // must copy because encoders may change the buffer
      encoder.encode(IntsRef.deepCopyOf(data[idx]), buf);
      exhausted = true;
      return true;
    }

  }

  static final IntsRef[] data = new IntsRef[] {
    new IntsRef(new int[] { 1, 2 }, 0, 2), 
    new IntsRef(new int[] { 3, 4 }, 0, 2),
    new IntsRef(new int[] { 1, 3 }, 0, 2),
    new IntsRef(new int[] { 1, 2, 3, 4 }, 0, 4)
  };

  @Test
  public void testPayloadCategoryListIteraor() throws Exception {
    Directory dir = newDirectory();
    final IntEncoder encoder = new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEncoder())));
    DataTokenStream dts = new DataTokenStream("1",encoder);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < data.length; i++) {
      dts.setIdx(i);
      Document doc = new Document();
      doc.add(new TextField("f", dts));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    int totalCategories = 0;
    IntsRef ordinals = new IntsRef();
    CategoryListIterator cli = new PayloadCategoryListIteraor(new Term("f","1"), encoder.createMatchingDecoder());
    for (AtomicReaderContext context : reader.leaves()) {
      cli.setNextReader(context);
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
  public void testPayloadIteratorWithInvalidDoc() throws Exception {
    Directory dir = newDirectory();
    final IntEncoder encoder = new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEncoder())));
    DataTokenStream dts = new DataTokenStream("1", encoder);
    // this test requires that no payloads ever be randomly present!
    final Analyzer noPayloadsAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new MockTokenizer(reader, MockTokenizer.KEYWORD, false));
      }
    };
    // NOTE: test is wired to LogMP... because test relies on certain docids having payloads
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, noPayloadsAnalyzer).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < data.length; i++) {
      Document doc = new Document();
      if (i == 0) {
        dts.setIdx(i);
        doc.add(new TextField("f", dts)); // only doc 0 has payloads!
      } else {
        doc.add(new TextField("f", "1", Field.Store.NO));
      }
      writer.addDocument(doc);
      writer.commit();
    }

    IndexReader reader = writer.getReader();
    writer.close();

    int totalCategories = 0;
    IntsRef ordinals = new IntsRef();
    CategoryListIterator cli = new PayloadCategoryListIteraor(new Term("f","1"), encoder.createMatchingDecoder());
    for (AtomicReaderContext context : reader.leaves()) {
      cli.setNextReader(context);
      int maxDoc = context.reader().maxDoc();
      int dataIdx = context.docBase;
      for (int doc = 0; doc < maxDoc; doc++, dataIdx++) {
        Set<Integer> values = new HashSet<Integer>();
        for (int j = 0; j < data[dataIdx].length; j++) {
          values.add(data[dataIdx].ints[j]);
        }
        cli.getOrdinals(doc, ordinals);
        if (dataIdx == 0) {
          assertTrue("document 0 must have a payload", ordinals.length > 0);
          for (int j = 0; j < ordinals.length; j++) {
            assertTrue("expected category not found: " + ordinals.ints[j], values.contains(ordinals.ints[j]));
          }
          totalCategories += ordinals.length;
        } else {
          assertTrue("only document 0 should have a payload", ordinals.length == 0);
        }
      }
    }
    assertEquals("Wrong number of total categories!", 2, totalCategories);

    reader.close();
    dir.close();
  }

}
