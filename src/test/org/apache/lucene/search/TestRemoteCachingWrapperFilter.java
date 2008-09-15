package org.apache.lucene.search;

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

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

/**
 * Tests that the index is cached on the searcher side of things.
 * NOTE: This is copied from TestRemoteSearchable since it already had a remote index set up.
 */
public class TestRemoteCachingWrapperFilter extends LuceneTestCase {
  public TestRemoteCachingWrapperFilter(String name) {
    super(name);
  }

  private static Searchable getRemote() throws Exception {
    try {
      return lookupRemote();
    } catch (Throwable e) {
      startServer();
      return lookupRemote();
    }
  }

  private static Searchable lookupRemote() throws Exception {
    return (Searchable)Naming.lookup("//localhost/Searchable");
  }

  private static void startServer() throws Exception {
    // construct an index
    RAMDirectory indexStore = new RAMDirectory();
    IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true,
                                         IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("test", "test text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("type", "A", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("other", "other test text", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    //Need a second document to search for
    doc = new Document();
    doc.add(new Field("test", "test text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("type", "B", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("other", "other test text", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.optimize();
    writer.close();

    // publish it
    LocateRegistry.createRegistry(1099);
    Searchable local = new IndexSearcher(indexStore);
    RemoteSearchable impl = new RemoteSearchable(local);
    Naming.rebind("//localhost/Searchable", impl);
  }

  private static void search(Query query, Filter filter, int hitNumber, String typeValue) throws Exception {
    Searchable[] searchables = { getRemote() };
    Searcher searcher = new MultiSearcher(searchables);
    ScoreDoc[] result = searcher.search(query,filter, 1000).scoreDocs;
    assertEquals(1, result.length);
    Document document = searcher.doc(result[hitNumber].doc);
    assertTrue("document is null and it shouldn't be", document != null);
    assertEquals(typeValue, document.get("type"));
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 3, document.getFields().size() == 3);
  }


  public void testTermRemoteFilter() throws Exception {
    CachingWrapperFilterHelper cwfh = new CachingWrapperFilterHelper(new QueryWrapperFilter(new TermQuery(new Term("type", "a"))));
    
    // This is what we are fixing - if one uses a CachingWrapperFilter(Helper) it will never 
    // cache the filter on the remote site
    cwfh.setShouldHaveCache(false);
    search(new TermQuery(new Term("test", "test")), cwfh, 0, "A");
    cwfh.setShouldHaveCache(false);
    search(new TermQuery(new Term("test", "test")), cwfh, 0, "A");
    
    // This is how we fix caching - we wrap a Filter in the RemoteCachingWrapperFilter(Handler - for testing)
    // to cache the Filter on the searcher (remote) side
    RemoteCachingWrapperFilterHelper rcwfh = new RemoteCachingWrapperFilterHelper(cwfh, false);
    search(new TermQuery(new Term("test", "test")), rcwfh, 0, "A");

    // 2nd time we do the search, we should be using the cached Filter
    rcwfh.shouldHaveCache(true);
    search(new TermQuery(new Term("test", "test")), rcwfh, 0, "A");

    // assert that we get the same cached Filter, even if we create a new instance of RemoteCachingWrapperFilter(Helper)
    // this should pass because the Filter parameters are the same, and the cache uses Filter's hashCode() as cache keys,
    // and Filters' hashCode() builds on Filter parameters, not the Filter instance itself
    rcwfh = new RemoteCachingWrapperFilterHelper(new QueryWrapperFilter(new TermQuery(new Term("type", "a"))), false);
    rcwfh.shouldHaveCache(false);
    search(new TermQuery(new Term("test", "test")), rcwfh, 0, "A");

    rcwfh = new RemoteCachingWrapperFilterHelper(new QueryWrapperFilter(new TermQuery(new Term("type", "a"))), false);
    rcwfh.shouldHaveCache(true);
    search(new TermQuery(new Term("test", "test")), rcwfh, 0, "A");

    // assert that we get a non-cached version of the Filter because this is a new Query (type:b)
    rcwfh = new RemoteCachingWrapperFilterHelper(new QueryWrapperFilter(new TermQuery(new Term("type", "b"))), false);
    rcwfh.shouldHaveCache(false);
    search(new TermQuery(new Term("type", "b")), rcwfh, 0, "B");
  }
}
