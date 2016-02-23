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

import java.net.InetAddress;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for {@link InetAddressPoint} */
public class TestInetAddressPoint extends LuceneTestCase {

  /** Add a single address and search for it */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with an address
    Document document = new Document();
    InetAddress address = InetAddress.getByName("1.2.3.4");
    document.add(new InetAddressPoint("field", address));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressExact("field", address)));
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressPrefix("field", address, 24)));
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressRange("field", InetAddress.getByName("1.2.3.3"), false, InetAddress.getByName("1.2.3.5"), false)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** Add a single address and search for it */
  public void testBasicsV6() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with an address
    Document document = new Document();
    InetAddress address = InetAddress.getByName("fec0::f66d");
    document.add(new InetAddressPoint("field", address));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressExact("field", address)));
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressPrefix("field", address, 64)));
    assertEquals(1, searcher.count(InetAddressPoint.newInetAddressRange("field", InetAddress.getByName("fec0::f66c"), false, InetAddress.getByName("fec0::f66e"), false)));

    reader.close();
    writer.close();
    dir.close();
  }
    
  public void testToString() throws Exception {
    assertEquals("<field:1.2.3.4>", new InetAddressPoint("field", InetAddress.getByName("1.2.3.4")).toString());
    assertEquals("<field:1.2.3.4>", new InetAddressPoint("field", InetAddress.getByName("::FFFF:1.2.3.4")).toString());
    assertEquals("<field:[fdc8:57ed:f042:ad1:f66d:4ff:fe90:ce0c]>", new InetAddressPoint("field", InetAddress.getByName("fdc8:57ed:f042:0ad1:f66d:4ff:fe90:ce0c")).toString());
    
    assertEquals("field:[1.2.3.4 TO 1.2.3.4]", InetAddressPoint.newInetAddressExact("field", InetAddress.getByName("1.2.3.4")).toString());
    assertEquals("field:[0:0:0:0:0:0:0:1 TO 0:0:0:0:0:0:0:1]", InetAddressPoint.newInetAddressExact("field", InetAddress.getByName("::1")).toString());
    
    assertEquals("field:[1.2.3.0 TO 1.2.3.255]", InetAddressPoint.newInetAddressPrefix("field", InetAddress.getByName("1.2.3.4"), 24).toString());
    assertEquals("field:[fdc8:57ed:f042:ad1:0:0:0:0 TO fdc8:57ed:f042:ad1:ffff:ffff:ffff:ffff]", InetAddressPoint.newInetAddressPrefix("field", InetAddress.getByName("fdc8:57ed:f042:0ad1:f66d:4ff:fe90:ce0c"), 64).toString());
  }
}
