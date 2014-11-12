package org.apache.lucene.search.posfilter;
/**
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;

import java.io.IOException;

public class TestPhraseQueryPositions extends IntervalTestBase {
  
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
        //  0       1      2     3     4       5     6      7      8   9  10   11   12   13
          "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! "
        //  14   15  16 17    18   19  20  21    22   23  24 25 26  27   28   29   30
        + "Some like it hot, some like it cold, Some like it in the pot nine days old! "
        //  31      32     33    34     35     36
        + "Pease porridge hot! Pease porridge cold!",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
        //  0       1      2     3     4       5     6      7      8   9  10   11   12   13
          "Pease porridge cold! Pease porridge hot! Pease porridge in the pot nine days old! "
        //  14   15  16 17    18   19  20  21    22   23  24 25 26  27   28   29   30
        + "Some like it cold, some like it hot, Some like it in the pot nine days old! "
        //  31      32     33    34     35     36
        + "Pease porridge cold! Pease porridge hot!",
          TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }

  public void testOutOfOrderSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "cold!"));
    query.add(new Term("field", "porridge"));
    query.setSlop(2);
    checkIntervals(query, searcher, new int[][]{
        {0, 3, 5, 3, 7, 5, 7, 34, 36},
        {1, 0, 2, 0, 4, 2, 4, 31, 33, 31, 35, 33, 35 }
    });
  }

  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "hot!"));
    query.setSlop(1);
    checkIntervals(query, searcher, new int[][]{
        {0, 0, 2, 31, 33},
        {1, 3, 5, 34, 36}
    });
  }

  public void testManyTermSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "pot"));
    query.setSlop(2);
    checkIntervals(query, searcher, new int[][]{
        {0, 6, 10},
        {1, 6, 10}
    });
  }

  public void testMultiPhrases() throws IOException {

    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(new Term("field", "pease"));
    q.add(new Term("field", "porridge"));
    q.add(new Term[]{ new Term("field", "hot!"), new Term("field", "cold!") });

    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2, 3, 5, 31, 33, 34, 36 },
        { 1, 0, 2, 3, 5, 31, 33, 34, 36 }
    });
  }

  public void testOverlaps() throws IOException {
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "some"));
    q.add(new Term("field", "like"));
    q.add(new Term("field", "it"));
    q.add(new Term("field", "cold,"));
    q.add(new Term("field", "some"));
    q.add(new Term("field", "like"));
    checkIntervals(q, searcher, new int[][]{
        {0, 18, 23},
        {1, 14, 19}
    });
  }

  public void testMatching() throws IOException {

    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "pease"));
    q.add(new Term("field", "porridge"));
    q.add(new Term("field", "hot!"));

    checkIntervals(q, searcher, new int[][]{
        {0, 0, 2, 31, 33},
        {1, 3, 5, 34, 36}
    });

  }

  public void testPartialMatching() throws IOException {

    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "pease"));
    q.add(new Term("field", "porridge"));
    q.add(new Term("field", "hot!"));
    q.add(new Term("field", "pease"));
    q.add(new Term("field", "porridge"));
    q.add(new Term("field", "cold!"));

    checkIntervals(q, searcher, new int[][]{
        {0, 0, 5, 31, 36},
    });

  }

  public void testNonMatching() throws IOException {

    PhraseQuery q = new PhraseQuery();
    q.add(new Term("field", "pease"));
    q.add(new Term("field", "hot!"));

    checkIntervals(q, searcher, new int[][]{});

  }


}
