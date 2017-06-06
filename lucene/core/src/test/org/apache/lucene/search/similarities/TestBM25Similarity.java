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
package org.apache.lucene.search.similarities;


import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestBM25Similarity extends LuceneTestCase {
  
  public void testIllegalK1() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(Float.POSITIVE_INFINITY, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(-1, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(Float.NaN, 0.75f);
    });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
  }
  
  public void testIllegalB() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, 2f);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, -1f);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      new BM25Similarity(1.2f, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("illegal b value"));
  }

  public void testLengthEncodingBackwardCompatibility() throws IOException {
    Similarity similarity = new BM25Similarity();
    for (int indexCreatedVersionMajor : new int[] { Version.LUCENE_6_0_0.major, Version.LATEST.major}) {
      for (int length : new int[] {1, 2, 4}) { // these length values are encoded accurately on both cases
        Directory dir = newDirectory();
        // set the version on the directory
        new SegmentInfos(indexCreatedVersionMajor).commit(dir);
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(similarity));
        Document doc = new Document();
        String value = IntStream.range(0, length).mapToObj(i -> "b").collect(Collectors.joining(" "));
        doc.add(new TextField("foo", value, Store.NO));
        w.addDocument(doc);
        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(similarity);
        Explanation expl = searcher.explain(new TermQuery(new Term("foo", "b")), 0);
        Explanation docLen = findExplanation(expl, "fieldLength");
        assertNotNull(docLen);
        assertEquals(docLen.toString(), length, (int) docLen.getValue());
        w.close();
        reader.close();
        dir.close();
      }
    }
  }

  private static Explanation findExplanation(Explanation expl, String text) {
    if (expl.getDescription().equals(text)) {
      return expl;
    } else {
      for (Explanation sub : expl.getDetails()) {
        Explanation match = findExplanation(sub, text);
        if (match != null) {
          return match;
        }
      }
    }
    return null;
  }
}
