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
package org.apache.lucene.document;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for {@link LatLonPoint} */
public class TestLatLonPoint extends LuceneTestCase {

  /** Add a single address and search for it in a box */
  // NOTE: we don't currently supply an exact search, only ranges, because of the lossiness...
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with an address
    Document document = new Document();
    document.add(new LatLonPoint("field", 18.313694, -65.227444));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("field", 18, 19, -66, -65)));

    reader.close();
    writer.close();
    dir.close();
  }
    
  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("<field:18.313693958334625,-65.22744392976165>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[17.99999997485429 TO 18.999999999068677},[-65.9999999217689 TO -64.99999998137355}", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
  }
}
