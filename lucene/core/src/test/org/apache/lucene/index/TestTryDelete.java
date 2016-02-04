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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;


public class TestTryDelete extends LuceneTestCase
{
  private static IndexWriter getWriter (Directory directory)
    throws IOException
  {
    MergePolicy policy = new LogByteSizeMergePolicy();
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(policy);
    conf.setOpenMode(OpenMode.CREATE_OR_APPEND);

    IndexWriter writer = new IndexWriter(directory, conf);

    return writer;
  }

  private static Directory createIndex ()
    throws IOException
  {
    Directory directory = new RAMDirectory();

    IndexWriter writer = getWriter(directory);

    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new StringField("foo", String.valueOf(i), Store.YES));
      writer.addDocument(doc);
    }

    writer.commit();
    writer.close();

    return directory;
  }

  public void testTryDeleteDocument ()
    throws IOException
  {
    Directory directory = createIndex();

    IndexWriter writer = getWriter(directory);

    ReferenceManager<IndexSearcher> mgr = new SearcherManager(writer,
                                                              new SearcherFactory());

    TrackingIndexWriter mgrWriter = new TrackingIndexWriter(writer);

    IndexSearcher searcher = mgr.acquire();

    TopDocs topDocs = searcher.search(new TermQuery(new Term("foo", "0")),
                                      100);
    assertEquals(1, topDocs.totalHits);

    long result;
    if (random().nextBoolean()) {
      IndexReader r = DirectoryReader.open(writer);
      result = mgrWriter.tryDeleteDocument(r, 0);
      r.close();
    } else {
      result = mgrWriter.tryDeleteDocument(searcher.getIndexReader(), 0);
    }

    // The tryDeleteDocument should have succeeded:
    assertTrue(result != -1);

    assertTrue(writer.hasDeletions());

    if (random().nextBoolean()) {
      writer.commit();
    }

    assertTrue(writer.hasDeletions());
    
    mgr.maybeRefresh();

    searcher = mgr.acquire();

    topDocs = searcher.search(new TermQuery(new Term("foo", "0")), 100);

    assertEquals(0, topDocs.totalHits);
  }

  public void testTryDeleteDocumentCloseAndReopen ()
    throws IOException
  {
    Directory directory = createIndex();

    IndexWriter writer = getWriter(directory);

    ReferenceManager<IndexSearcher> mgr = new SearcherManager(writer,
                                                              new SearcherFactory());

    IndexSearcher searcher = mgr.acquire();

    TopDocs topDocs = searcher.search(new TermQuery(new Term("foo", "0")),
                                      100);
    assertEquals(1, topDocs.totalHits);

    TrackingIndexWriter mgrWriter = new TrackingIndexWriter(writer);
    long result = mgrWriter.tryDeleteDocument(DirectoryReader.open(writer), 0);

    assertEquals(1, result);

    writer.commit();

    assertTrue(writer.hasDeletions());

    mgr.maybeRefresh();

    searcher = mgr.acquire();

    topDocs = searcher.search(new TermQuery(new Term("foo", "0")), 100);

    assertEquals(0, topDocs.totalHits);

    writer.close();

    searcher = new IndexSearcher(DirectoryReader.open(directory));

    topDocs = searcher.search(new TermQuery(new Term("foo", "0")), 100);

    assertEquals(0, topDocs.totalHits);

  }

  public void testDeleteDocuments ()
    throws IOException
  {
    Directory directory = createIndex();

    IndexWriter writer = getWriter(directory);

    ReferenceManager<IndexSearcher> mgr = new SearcherManager(writer,
                                                              new SearcherFactory());

    IndexSearcher searcher = mgr.acquire();

    TopDocs topDocs = searcher.search(new TermQuery(new Term("foo", "0")),
                                      100);
    assertEquals(1, topDocs.totalHits);

    TrackingIndexWriter mgrWriter = new TrackingIndexWriter(writer);
    long result = mgrWriter.deleteDocuments(new TermQuery(new Term("foo",
                                                                   "0")));

    assertEquals(1, result);

    // writer.commit();

    assertTrue(writer.hasDeletions());

    mgr.maybeRefresh();

    searcher = mgr.acquire();

    topDocs = searcher.search(new TermQuery(new Term("foo", "0")), 100);

    assertEquals(0, topDocs.totalHits);
  }
}
