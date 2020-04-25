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
package org.apache.solr.uninverting;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;

public class TestFieldCacheReopen extends SolrTestCase {
  
  // TODO: make a version of this that tests the same thing with UninvertingReader.wrap()
  
  // LUCENE-1579: Ensure that on a reopened reader, that any
  // shared segments reuse the doc values arrays in
  // FieldCache
  public void testFieldCacheReuseAfterReopen() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).
            setMergePolicy(newLogMergePolicy(10))
    );
    Document doc = new Document();
    doc.add(new IntPoint("number", 17));
    writer.addDocument(doc);
    writer.commit();
  
    // Open reader1
    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader r1 = getOnlyLeafReader(r);
    final NumericDocValues ints = FieldCache.DEFAULT.getNumerics(r1, "number", FieldCache.INT_POINT_PARSER);
    assertEquals(0, ints.nextDoc());
    assertEquals(17, ints.longValue());
  
    // Add new segment
    writer.addDocument(doc);
    writer.commit();
  
    // Reopen reader1 --> reader2
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);
    r.close();
    LeafReader sub0 = r2.leaves().get(0).reader();
    final NumericDocValues ints2 = FieldCache.DEFAULT.getNumerics(sub0, "number", FieldCache.INT_POINT_PARSER);
    r2.close();
    assertEquals(0, ints2.nextDoc());
    assertEquals(17, ints2.longValue());
  
    writer.close();
    dir.close();
  }
}
