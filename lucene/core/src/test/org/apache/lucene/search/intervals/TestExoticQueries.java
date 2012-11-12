package org.apache.lucene.search.intervals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;

import java.io.IOException;

/**
 * Copyright (c) 2012 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TestExoticQueries extends IntervalTestBase {

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newField(
        "field",
        "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
            + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
        TextField.TYPE_NOT_STORED));
    writer.addDocument(doc);
  }

  public void testExactPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "hot!"));
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 1, 1, 2, 2, 31, 33, 31, 31, 32, 32, 33, 33 }
    });
  }

  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "hot!"));
    query.setSlop(1);
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 2, 2, 31, 33, 31, 31, 33, 33 }
    });
  }

  public void testManyTermSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "pot"));
    query.setSlop(2);
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 31, 33 }
    });
  }

  public void testMultiTermPhraseQuery() throws IOException {
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term[] {new Term("field", "hot!"), new Term("field", "cold!")});
    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 1, 1, 2, 2,
             3, 5, 3, 3, 4, 4, 5, 5,
             31, 33, 31, 31, 32, 32, 33, 33,
             34, 36, 34, 34, 35, 35, 36, 36 }
    });
  }
}

