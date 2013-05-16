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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util._TestUtil;

public class TestIndexSearcher extends LuceneTestCase {
  Directory dir;
  IndexReader reader;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      iw.addDocument(doc);
    }
    reader = iw.getReader();
    iw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }
  
  // should not throw exception
  public void testHugeN() throws Exception {
    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                                   new LinkedBlockingQueue<Runnable>(),
                                   new NamedThreadFactory("TestIndexSearcher"));
    
    IndexSearcher searchers[] = new IndexSearcher[] {
        new IndexSearcher(reader),
        new IndexSearcher(reader, service)
    };
    Query queries[] = new Query[] {
        new MatchAllDocsQuery(),
        new TermQuery(new Term("field", "1"))
    };
    Sort sorts[] = new Sort[] {
        null,
        new Sort(new SortField("field2", SortField.Type.STRING))
    };
    Filter filters[] = new Filter[] {
        null,
        new QueryWrapperFilter(new TermQuery(new Term("field2", "true")))
    };
    ScoreDoc afters[] = new ScoreDoc[] {
        null,
        new FieldDoc(0, 0f, new Object[] { new BytesRef("boo!") })
    };
    
    for (IndexSearcher searcher : searchers) {
      for (ScoreDoc after : afters) {
        for (Query query : queries) {
          for (Sort sort : sorts) {
            for (Filter filter : filters) {
              searcher.search(query, Integer.MAX_VALUE);
              searcher.searchAfter(after, query, Integer.MAX_VALUE);
              searcher.search(query, filter, Integer.MAX_VALUE);
              searcher.searchAfter(after, query, filter, Integer.MAX_VALUE);
              if (sort != null) {
                searcher.search(query, Integer.MAX_VALUE, sort);
                searcher.search(query, filter, Integer.MAX_VALUE, sort);
                searcher.search(query, filter, Integer.MAX_VALUE, sort, true, true);
                searcher.search(query, filter, Integer.MAX_VALUE, sort, true, false);
                searcher.search(query, filter, Integer.MAX_VALUE, sort, false, true);
                searcher.search(query, filter, Integer.MAX_VALUE, sort, false, false);
                searcher.searchAfter(after, query, filter, Integer.MAX_VALUE, sort);
                searcher.searchAfter(after, query, filter, Integer.MAX_VALUE, sort, true, true);
                searcher.searchAfter(after, query, filter, Integer.MAX_VALUE, sort, true, false);
                searcher.searchAfter(after, query, filter, Integer.MAX_VALUE, sort, false, true);
                searcher.searchAfter(after, query, filter, Integer.MAX_VALUE, sort, false, false);
              }
            }
          }
        }
      }
    }
    
    _TestUtil.shutdownExecutorService(service);
  }
}
