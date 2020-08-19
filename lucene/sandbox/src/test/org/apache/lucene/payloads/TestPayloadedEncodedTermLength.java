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

package org.apache.lucene.payloads;

import java.io.IOException;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.CheckHits.checkHits;

public class TestPayloadedEncodedTermLength extends LuceneTestCase {

  public void testPhrasesInDecomposedTerms() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("my", 1, 0, 2),
        new Token("wi-fi", 1, 3, 8, 2),
        new Token("wifi", 0, 3, 8, 2),
        new Token("wi", 0, 3, 5),
        new Token("fi", 6, 8),
        new Token("is", 9, 11),
        new Token("called", 12, 18),
        new Token("wifi", 19, 23)
    );
    ts = new PayloadTokenLengthFilter(ts);


    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    Directory directory = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(new Field("field", ts, ft));
    riw.addDocument(doc);

    IndexReader reader = riw.getReader();

    IntervalsSource source = new PayloadLengthTermIntervalsSource(new BytesRef("wifi"));
    IntervalIterator it = source.intervals("field", reader.leaves().get(0));

    assertEquals(0, it.advance(0));
    assertEquals(1, it.nextInterval());
    assertEquals(2, it.end());
    assertEquals(5, it.nextInterval());
    assertEquals(5, it.end());

    source = Intervals.phrase(Intervals.term("my"), source, Intervals.term("is"));
    Query q = new IntervalQuery("field", source);
    checkHits(random(), q, "field", new IndexSearcher(reader), new int[]{ 0 });

    reader.close();
    riw.close();
    directory.close();
  }

}
