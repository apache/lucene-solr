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


import java.io.FilePermission;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.PropertyPermission;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestReadOnlyIndex extends LuceneTestCase {

  private static final String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
  private static final String text = "This is the text to be indexed. " + longTerm;

  private static Path indexPath;  

  @BeforeClass
  public static void buildIndex() throws Exception {
    indexPath = Files.createTempDirectory("readonlyindex").toAbsolutePath();
    
    // borrows from TestDemo, but not important to keep in sync with demo
    Analyzer analyzer = new MockAnalyzer(random());
    Directory directory = newFSDirectory(indexPath);
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, analyzer);
    Document doc = new Document();
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    iwriter.addDocument(doc);
    iwriter.close();
    directory.close();
    analyzer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    indexPath = null;
  }
  
  public void testReadOnlyIndex() throws Exception {
    runWithRestrictedPermissions(this::doTestReadOnlyIndex,
        // add some basic permissions (because we are limited already - so we grant all important ones):
        new RuntimePermission("*"),
        new PropertyPermission("*", "read"),
        // only allow read to the given index dir, nothing else:
        new FilePermission(indexPath.toString(), "read"),
        new FilePermission(indexPath.resolve("-").toString(), "read")
    );
  }
  
  private Void doTestReadOnlyIndex() throws Exception {
    Directory dir = FSDirectory.open(indexPath); 
    IndexReader ireader = DirectoryReader.open(dir); 
    IndexSearcher isearcher = newSearcher(ireader);
    
    // borrows from TestDemo, but not important to keep in sync with demo

    assertEquals(1, isearcher.count(new TermQuery(new Term("fieldname", longTerm))));
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, 1);
    assertEquals(1, hits.totalHits.value);
    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
    }

    // Test simple phrase query
    PhraseQuery phraseQuery = new PhraseQuery("fieldname", "to", "be");
    assertEquals(1, isearcher.count(phraseQuery));

    ireader.close();
    return null; // void
  }
  
}
