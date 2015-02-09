package org.apache.lucene.search;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.io.IOException;

public class TestTermQueryPostings extends LuceneTestCase {

  private static final String FIELD = "f";

  private static final String[] DOC_FIELDS = new String[]{
      "a b c d",
      "a a a a",
      "c d e f",
      "b d a g"
  };

  @Test
  public void testTermQueryPositions() throws IOException {

    Directory directory = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config);

    for (String content : DOC_FIELDS) {
      Document doc = new Document();
      doc.add(newField(FIELD, content, TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }


    IndexReader reader = SlowCompositeReaderWrapper.wrap(writer.getReader());
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();

    TermQuery tq = new TermQuery(new Term(FIELD, "a"));
    Weight weight = searcher.createNormalizedWeight(tq, PostingsEnum.FLAG_POSITIONS);
    LeafReaderContext ctx = (LeafReaderContext) searcher.getTopReaderContext();
    Scorer scorer = weight.scorer(ctx, null);

    assertEquals(scorer.nextDoc(), 0);
    assertEquals(scorer.nextPosition(), 0);

    assertEquals(scorer.nextDoc(), 1);
    assertEquals(scorer.nextPosition(), 0);
    assertEquals(scorer.nextPosition(), 1);
    assertEquals(scorer.nextPosition(), 2);
    assertEquals(scorer.nextPosition(), 3);

    assertEquals(scorer.nextDoc(), 3);
    assertEquals(scorer.nextPosition(), 2);

    reader.close();
    directory.close();

  }

  @Test
  public void testTermQueryOffsets() throws IOException {

    Directory directory = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config);

    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

    for (String content : DOC_FIELDS) {
      Document doc = new Document();
      doc.add(newField(FIELD, content, fieldType));
      writer.addDocument(doc);
    }


    IndexReader reader = SlowCompositeReaderWrapper.wrap(writer.getReader());
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();

    TermQuery tq = new TermQuery(new Term(FIELD, "a"));
    Weight weight = searcher.createNormalizedWeight(tq, PostingsEnum.FLAG_OFFSETS);
    LeafReaderContext ctx = (LeafReaderContext) searcher.getTopReaderContext();
    Scorer scorer = weight.scorer(ctx, null);

    assertEquals(scorer.nextDoc(), 0);
    assertEquals(scorer.nextPosition(), 0);
    assertEquals(scorer.startOffset(), 0);
    assertEquals(scorer.endOffset(), 1);

    assertEquals(scorer.nextDoc(), 1);
    assertEquals(scorer.nextPosition(), 0);
    assertEquals(scorer.startOffset(), 0);
    assertEquals(scorer.endOffset(), 1);
    assertEquals(scorer.nextPosition(), 1);
    assertEquals(scorer.startOffset(), 2);
    assertEquals(scorer.endOffset(), 3);
    assertEquals(scorer.nextPosition(), 2);
    assertEquals(scorer.startOffset(), 4);
    assertEquals(scorer.endOffset(), 5);
    assertEquals(scorer.nextPosition(), 3);
    assertEquals(scorer.startOffset(), 6);
    assertEquals(scorer.endOffset(), 7);

    assertEquals(scorer.nextDoc(), 3);
    assertEquals(scorer.nextPosition(), 2);
    assertEquals(scorer.startOffset(), 4);
    assertEquals(scorer.endOffset(), 5);

    reader.close();
    directory.close();

  }

  @Test
  public void testTermQueryPayloads() throws Exception {

    PayloadHelper helper = new PayloadHelper();
    IndexSearcher searcher = helper.setUp(random(), new DefaultSimilarity(), 1000);

    TermQuery tq = new TermQuery(new Term(PayloadHelper.FIELD, "seventy"));
    Weight weight = searcher.createNormalizedWeight(tq, PostingsEnum.FLAG_PAYLOADS);

    for (LeafReaderContext ctx : searcher.leafContexts) {
      Scorer scorer = weight.scorer(ctx, null);
      if (scorer.nextDoc() == DocIdSetIterator.NO_MORE_DOCS)
        continue;
      scorer.nextPosition();
      BytesRef payload = scorer.getPayload();
      assertEquals(payload.length, 1);
    }

    helper.tearDown();
  }
}
