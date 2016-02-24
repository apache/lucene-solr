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

import java.math.BigInteger;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Simple tests for {@link BigIntegerPoint} */
public class TestBigIntegerPoint extends LuceneTestCase {

  /** Add a single 1D point and search for it */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a large biginteger value
    Document document = new Document();
    BigInteger large = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(64));
    document.add(new BigIntegerPoint("field", large));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(BigIntegerPoint.newExactQuery("field", large)));
    assertEquals(1, searcher.count(BigIntegerPoint.newRangeQuery("field", large.subtract(BigInteger.ONE), false, large.add(BigInteger.ONE), false)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** Add a negative 1D point and search for it */
  public void testNegative() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a large biginteger value
    Document document = new Document();
    BigInteger negative = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(64)).negate();
    document.add(new BigIntegerPoint("field", negative));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(BigIntegerPoint.newExactQuery("field", negative)));
    assertEquals(1, searcher.count(BigIntegerPoint.newRangeQuery("field", negative.subtract(BigInteger.ONE), false, negative.add(BigInteger.ONE), false)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** Test if we add a too-large value */
  public void testTooLarge() throws Exception {
    BigInteger tooLarge = BigInteger.ONE.shiftLeft(128);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new BigIntegerPoint("field", tooLarge);
    });
    assertTrue(expected.getMessage().contains("requires more than 16 bytes storage"));
  }
  
  public void testToString() throws Exception {
    assertEquals("BigIntegerPoint <field:1>", new BigIntegerPoint("field", BigInteger.ONE).toString());
    assertEquals("BigIntegerPoint <field:1,-2>", new BigIntegerPoint("field", BigInteger.ONE, BigInteger.valueOf(-2)).toString());
  }
}
