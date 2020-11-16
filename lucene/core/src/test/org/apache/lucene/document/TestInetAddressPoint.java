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
import org.apache.lucene.search.Query;
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
    assertEquals(1, searcher.count(InetAddressPoint.newExactQuery("field", address)));
    assertEquals(1, searcher.count(InetAddressPoint.newPrefixQuery("field", address, 24)));
    assertEquals(1, searcher.count(InetAddressPoint.newRangeQuery("field", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"))));
    assertEquals(1, searcher.count(InetAddressPoint.newSetQuery("field", InetAddress.getByName("1.2.3.4"))));
    assertEquals(1, searcher.count(InetAddressPoint.newSetQuery("field", InetAddress.getByName("1.2.3.4"), InetAddress.getByName("1.2.3.5"))));
    assertEquals(0, searcher.count(InetAddressPoint.newSetQuery("field", InetAddress.getByName("1.2.3.3"))));
    assertEquals(0, searcher.count(InetAddressPoint.newSetQuery("field")));

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
    assertEquals(1, searcher.count(InetAddressPoint.newExactQuery("field", address)));
    assertEquals(1, searcher.count(InetAddressPoint.newPrefixQuery("field", address, 64)));
    assertEquals(1, searcher.count(InetAddressPoint.newRangeQuery("field", InetAddress.getByName("fec0::f66c"), InetAddress.getByName("fec0::f66e"))));

    reader.close();
    writer.close();
    dir.close();
  }
    
  public void testToString() throws Exception {
    assertEquals("InetAddressPoint <field:1.2.3.4>", new InetAddressPoint("field", InetAddress.getByName("1.2.3.4")).toString());
    assertEquals("InetAddressPoint <field:1.2.3.4>", new InetAddressPoint("field", InetAddress.getByName("::FFFF:1.2.3.4")).toString());
    assertEquals("InetAddressPoint <field:[fdc8:57ed:f042:ad1:f66d:4ff:fe90:ce0c]>", new InetAddressPoint("field", InetAddress.getByName("fdc8:57ed:f042:0ad1:f66d:4ff:fe90:ce0c")).toString());
    
    assertEquals("field:[1.2.3.4 TO 1.2.3.4]", InetAddressPoint.newExactQuery("field", InetAddress.getByName("1.2.3.4")).toString());
    assertEquals("field:[0:0:0:0:0:0:0:1 TO 0:0:0:0:0:0:0:1]", InetAddressPoint.newExactQuery("field", InetAddress.getByName("::1")).toString());
    
    assertEquals("field:[1.2.3.0 TO 1.2.3.255]", InetAddressPoint.newPrefixQuery("field", InetAddress.getByName("1.2.3.4"), 24).toString());
    assertEquals("field:[fdc8:57ed:f042:ad1:0:0:0:0 TO fdc8:57ed:f042:ad1:ffff:ffff:ffff:ffff]", InetAddressPoint.newPrefixQuery("field", InetAddress.getByName("fdc8:57ed:f042:0ad1:f66d:4ff:fe90:ce0c"), 64).toString());
    assertEquals("field:{fdc8:57ed:f042:ad1:f66d:4ff:fe90:ce0c}", InetAddressPoint.newSetQuery("field", InetAddress.getByName("fdc8:57ed:f042:0ad1:f66d:4ff:fe90:ce0c")).toString());
  }

  public void testQueryEquals() throws Exception {
    Query q1, q2;
    q1 = InetAddressPoint.newRangeQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"));
    q2 = InetAddressPoint.newRangeQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"));
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(InetAddressPoint.newRangeQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.7"))));
    assertFalse(q1.equals(InetAddressPoint.newRangeQuery("b", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"))));

    q1 = InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.2.3.3"), 16);
    q2 = InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.2.3.3"), 16);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.1.3.5"), 16)));
    assertFalse(q1.equals(InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.2.3.5"), 24)));

    q1 = InetAddressPoint.newExactQuery("a", InetAddress.getByName("1.2.3.3"));
    q2 = InetAddressPoint.newExactQuery("a", InetAddress.getByName("1.2.3.3"));
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(InetAddressPoint.newExactQuery("a", InetAddress.getByName("1.2.3.5"))));

    q1 = InetAddressPoint.newSetQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"));
    q2 = InetAddressPoint.newSetQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.5"));
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(InetAddressPoint.newSetQuery("a", InetAddress.getByName("1.2.3.3"), InetAddress.getByName("1.2.3.7"))));
  }

  public void testPrefixQuery() throws Exception {
    assertEquals(
        InetAddressPoint.newRangeQuery("a", InetAddress.getByName("1.2.3.0"), InetAddress.getByName("1.2.3.255")),
        InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.2.3.127"), 24));
    assertEquals(
        InetAddressPoint.newRangeQuery("a", InetAddress.getByName("1.2.3.128"), InetAddress.getByName("1.2.3.255")),
        InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("1.2.3.213"), 25));
    assertEquals(
        InetAddressPoint.newRangeQuery("a", InetAddress.getByName("2001::a000:0"), InetAddress.getByName("2001::afff:ffff")),
        InetAddressPoint.newPrefixQuery("a", InetAddress.getByName("2001::a6bd:fc80"), 100));
  }

  public void testNextUp() throws Exception {
    assertEquals(InetAddress.getByName("::1"),
        InetAddressPoint.nextUp(InetAddress.getByName("::")));

    assertEquals(InetAddress.getByName("::1:0"),
        InetAddressPoint.nextUp(InetAddress.getByName("::ffff")));

    assertEquals(InetAddress.getByName("1.2.4.0"),
        InetAddressPoint.nextUp(InetAddress.getByName("1.2.3.255")));

    assertEquals(InetAddress.getByName("0.0.0.0"),
        InetAddressPoint.nextUp(InetAddress.getByName("::fffe:ffff:ffff")));

    assertEquals(InetAddress.getByName("::1:0:0:0"),
        InetAddressPoint.nextUp(InetAddress.getByName("255.255.255.255")));

    ArithmeticException e = expectThrows(ArithmeticException.class,
        () -> InetAddressPoint.nextUp(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")));
    assertEquals("Overflow: there is no greater InetAddress than ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", e.getMessage());
  }

  public void testNextDown() throws Exception {
    assertEquals(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"),
        InetAddressPoint.nextDown(InetAddress.getByName("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")));

    assertEquals(InetAddress.getByName("::ffff"),
        InetAddressPoint.nextDown(InetAddress.getByName("::1:0")));

    assertEquals(InetAddress.getByName("1.2.3.255"),
        InetAddressPoint.nextDown(InetAddress.getByName("1.2.4.0")));

    assertEquals(InetAddress.getByName("::fffe:ffff:ffff"),
        InetAddressPoint.nextDown(InetAddress.getByName("0.0.0.0")));

    assertEquals(InetAddress.getByName("255.255.255.255"),
        InetAddressPoint.nextDown(InetAddress.getByName("::1:0:0:0")));

    ArithmeticException e = expectThrows(ArithmeticException.class,
        () -> InetAddressPoint.nextDown(InetAddress.getByName("::")));
    assertEquals("Underflow: there is no smaller InetAddress than 0:0:0:0:0:0:0:0", e.getMessage());
  }
}
