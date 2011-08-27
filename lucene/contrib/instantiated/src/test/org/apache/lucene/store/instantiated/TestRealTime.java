/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store.instantiated;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Assert that the content of an index 
 * is instantly available
 * for all open searchers
 * also after a commit.
 */
public class TestRealTime extends LuceneTestCase {

  public void test() throws Exception {

    InstantiatedIndex index = new InstantiatedIndex();
    InstantiatedIndexReader reader = new InstantiatedIndexReader(index);
    IndexSearcher searcher = newSearcher(reader, false);
    InstantiatedIndexWriter writer = new InstantiatedIndexWriter(index);

    Document doc;
    Collector collector;

    doc = new Document();
    doc.add(new StringField("f", "a"));
    writer.addDocument(doc);
    writer.commit();

    collector = new Collector();
    searcher.search(new TermQuery(new Term("f", "a")), collector);
    assertEquals(1, collector.hits);

    doc = new Document();
    doc.add(new StringField("f", "a"));
    writer.addDocument(doc);
    writer.commit();

    collector = new Collector();
    searcher.search(new TermQuery(new Term("f", "a")), collector);
    assertEquals(2, collector.hits);

  }

  public static class Collector extends org.apache.lucene.search.Collector {
    private int hits = 0;
    @Override
    public void setScorer(Scorer scorer) {}
    @Override
    public void setNextReader(AtomicReaderContext context) {}
    @Override
    public boolean acceptsDocsOutOfOrder() { return true; }
    @Override
    public void collect(int doc) {
      hits++;
    }
  }

}
