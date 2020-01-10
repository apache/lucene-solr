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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * 
 */
public class TestCustomNorms extends LuceneTestCase {
  static final String FLOAT_TEST_FIELD = "normsTestFloat";
  static final String EXCEPTION_TEST_FIELD = "normsTestExcp";

  public void testFloatNorms() throws IOException {

    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 2, IndexWriter.MAX_TERM_LENGTH));

    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    Similarity provider = new MySimProvider();
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    final LineFileDocs docs = new LineFileDocs(random());
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      Document doc = docs.nextDoc();
      int boost = TestUtil.nextInt(random(), 1, 10);
      String value = IntStream.range(0, boost).mapToObj(k -> Integer.toString(boost)).collect(Collectors.joining(" "));
      Field f = new TextField(FLOAT_TEST_FIELD, value, Field.Store.YES);

      doc.add(f);
      writer.addDocument(doc);
      doc.removeField(FLOAT_TEST_FIELD);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.commit();
    writer.close();
    DirectoryReader open = DirectoryReader.open(dir);
    NumericDocValues norms = MultiDocValues.getNormValues(open, FLOAT_TEST_FIELD);
    assertNotNull(norms);
    for (int i = 0; i < open.maxDoc(); i++) {
      Document document = open.document(i);
      int expected = Integer.parseInt(document.get(FLOAT_TEST_FIELD).split(" ")[0]);
      assertEquals(i, norms.nextDoc());
      assertEquals(expected, norms.longValue());
    }
    open.close();
    dir.close();
    docs.close();
  }

  public class MySimProvider extends PerFieldSimilarityWrapper {
    Similarity delegate = new ClassicSimilarity();

    @Override
    public Similarity get(String field) {
      if (FLOAT_TEST_FIELD.equals(field)) {
        return new FloatEncodingBoostSimilarity();
      } else {
        return delegate;
      }
    }
  }

  public static class FloatEncodingBoostSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return state.getLength();
    }
    
    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }
  }
}
