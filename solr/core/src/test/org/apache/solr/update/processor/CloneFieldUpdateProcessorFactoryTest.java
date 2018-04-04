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
package org.apache.solr.update.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloneFieldUpdateProcessorFactoryTest extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }

  @Test
  public void testSimpleClone() throws Exception {
    SolrInputDocument doc = processAdd("clone-single",
                                       doc(f("id", "1"),
                                           f("source1_s", "foo")
                                           ));
    assertEquals("source1_s should have stringValue", "foo", doc.getFieldValue("source1_s"));
    assertEquals("dest_s should have stringValue", "foo", doc.getFieldValue("dest_s"));
  }

  @Test
  public void testMultiClone() throws Exception {
    SolrInputDocument doc = processAdd("clone-multi",
                                       doc(f("id", "1"),
                                           f("source1_s", "foo"),
                                           f("source2_s", "bar")));
                                           
    assertEquals("source1_s should have stringValue", "foo", doc.getFieldValue("source1_s"));
    assertEquals("source2_s should have stringValue", "bar", doc.getFieldValue("source2_s"));
    Collection<Object> dest_s = doc.getFieldValues("dest_s");
    assertTrue(dest_s.contains("foo"));
    assertTrue(dest_s.contains("bar"));
  }

  @Test
  public void testArrayClone() throws Exception {
    SolrInputDocument doc = processAdd("clone-array",
                                       doc(f("id", "1"),
                                           f("source1_s", "foo"),
                                           f("source2_s", "bar")));
                                           
    assertEquals("source1_s should have stringValue", "foo", doc.getFieldValue("source1_s"));
    assertEquals("source2_s should have stringValue", "bar", doc.getFieldValue("source2_s"));
    Collection<Object> dest_s = doc.getFieldValues("dest_s");
    assertTrue(dest_s.contains("foo"));
    assertTrue(dest_s.contains("bar"));
  }

  @Test
  public void testSelectorClone() throws Exception {
    SolrInputDocument doc = processAdd("clone-selector",
                                       doc(f("id", "1"),
                                           f("source0_s", "nope, not me"),
                                           f("source1_s", "foo"),
                                           f("source2_s", "bar")));
                                           
    assertEquals("source0_s should have stringValue", "nope, not me", doc.getFieldValue("source0_s"));
    assertEquals("source1_s should have stringValue", "foo", doc.getFieldValue("source1_s"));
    assertEquals("source2_s should have stringValue", "bar", doc.getFieldValue("source2_s"));
    Collection<Object> dest_s = doc.getFieldValues("dest_s");
    assertTrue(dest_s.contains("foo"));
    assertTrue(dest_s.contains("bar"));
    assertFalse(dest_s.contains("nope, not me"));
  }

  public void testMultipleClones() throws Exception {
    SolrInputDocument doc = processAdd("multiple-clones",
                                       doc(f("id", "1"),
                                           f("category", "test"),
                                           f("authors", "author1", "author2"),
                                           f("editors", "ed1", "ed2"),
                                           f("bfriday_price", 4.00),
                                           f("sale_price", 5.00),
                                           f("list_price", 6.00),
                                           f("features", "hill", "valley", "dune")));
                                           
    // the original values should remain
    assertEquals("category should have a value", "test", doc.getFieldValue("category"));

    Collection<Object> auths = doc.getFieldValues("authors");
    assertTrue(auths.size() == 2);
    assertTrue(auths.contains("author1"));
    assertTrue(auths.contains("author2"));
    Collection<Object> eds = doc.getFieldValues("editors");
    assertTrue(eds.size() == 2);
    assertTrue(eds.contains("ed1"));
    assertTrue(eds.contains("ed2"));

    assertEquals("bfriday_price should have a value", 4.0, doc.getFieldValue("bfriday_price"));
    assertEquals("sale_price should have a value", 5.0, doc.getFieldValue("sale_price"));
    assertEquals("list_price should have a value", 6.0, doc.getFieldValue("list_price"));

    Collection<Object> features = doc.getFieldValues("features");
    assertTrue(features.size() == 3);
    assertTrue(features.contains("hill"));
    assertTrue(features.contains("valley"));
    assertTrue(features.contains("dune"));

    // and the copied values shoul be added
    assertEquals("category_s should have a value", "test", doc.getFieldValue("category_s"));

    Collection<Object> contribs = doc.getFieldValues("contributors");
    assertTrue(contribs.size() == 4);
    assertTrue(contribs.contains("author1"));
    assertTrue(contribs.contains("author2"));
    assertTrue(contribs.contains("ed1"));
    assertTrue(contribs.contains("ed2"));

    Collection<Object> prices = doc.getFieldValues("all_prices");
    assertTrue(prices.size() == 2);
    assertTrue(prices.contains(5.0));
    assertTrue(prices.contains(4.0));
    assertFalse(prices.contains(6.0));

    // n.b. the field names below imply singularity but that would be achieved with a subsequent 
    // FirstFieldValueUpdateProcessorFactory (or similar custom class), and not in clone field itself

    Collection<Object> keyf = doc.getFieldValues("key_feature");
    assertTrue(keyf.size() == 3);
    assertTrue(keyf.contains("hill"));
    assertTrue(keyf.contains("valley"));
    assertTrue(keyf.contains("dune"));

    Collection<Object> bestf = doc.getFieldValues("best_feature");
    assertTrue(bestf.size() == 3);
    assertTrue(bestf.contains("hill"));
    assertTrue(bestf.contains("valley"));
    assertTrue(bestf.contains("dune"));
  }

  public void testCloneField() throws Exception {

    SolrInputDocument d;

    // regardless of chain, all of these checks should be equivalent
    for (String chain : Arrays.asList("clone-single", "clone-single-regex",
                                      "clone-multi", "clone-multi-regex",
                                      "clone-array", "clone-array-regex", 
                                      "clone-selector", "clone-selector-regex")) {
      
      // simple clone
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("dest_s"));

      // append to existing values, preserve boost
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         field("dest_s", "orig1", "orig2"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("orig1", "orig2", "123456789", "", 42, "abcd"),
                   d.getFieldValues("dest_s"));
    }

    // should be equivalent for any chain matching source1_s and source2_s (but not source0_s)
    for (String chain : Arrays.asList("clone-multi", "clone-multi-regex",
                                      "clone-array", "clone-array-regex", 
                                      "clone-selector", "clone-selector-regex")) {

      // simple clone
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd"),
                         f("source2_s", "xxx", 999)));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("xxx", 999),
                   d.getFieldValues("source2_s"));
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd", "xxx", 999),
                   d.getFieldValues("dest_s"));

      // append to existing values
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         field("dest_s", "orig1", "orig2"),
                         f("source0_s", "NOT COPIED"),
                         f("source1_s", "123456789", "", 42, "abcd"),
                         f("source2_s", "xxx", 999)));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("xxx", 999),
                   d.getFieldValues("source2_s"));
      assertEquals(chain,
                   Arrays.asList("orig1", "orig2",
                                 "123456789", "", 42, "abcd",
                                 "xxx", 999),
                   d.getFieldValues("dest_s"));
    }
    
    // any chain that copies source1_s to dest_s should be equivalent for these assertions
    for (String chain : Arrays.asList("clone-simple-regex-syntax",
                                      "clone-single", "clone-single-regex",
                                      "clone-multi", "clone-multi-regex",
                                      "clone-array", "clone-array-regex", 
                                      "clone-selector", "clone-selector-regex")) {

      // simple clone
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("dest_s"));

      // append to existing values, preserve boost
      d = processAdd(chain,
                     doc(f("id", "1111"),
                         field("dest_s", "orig1", "orig2"),
                         f("source1_s", "123456789", "", 42, "abcd")));
      assertNotNull(chain, d);
      assertEquals(chain,
                   Arrays.asList("123456789", "", 42, "abcd"),
                   d.getFieldValues("source1_s"));
      assertEquals(chain,
                   Arrays.asList("orig1", "orig2", "123456789", "", 42, "abcd"),
                   d.getFieldValues("dest_s"));
    }
  }

  public void testCloneFieldRegexReplaceAll() throws Exception {
    SolrInputDocument d = processAdd("clone-regex-replaceall",
                                     doc(f("id", "1111"),
                                         f("foo_x2_s", "123456789", "", 42, "abcd"),
                                         f("foo_x3_x7_s", "xyz")));
    
    assertNotNull(d);
    assertEquals(Arrays.asList("123456789", "", 42, "abcd"),
                 d.getFieldValues("foo_y2_s"));
    assertEquals("xyz",
                 d.getFieldValue("foo_y3_y7_s"));
  }
  
  public void testCloneFieldExample() throws Exception {

    SolrInputDocument d;

    // test example from the javadocs
    d = processAdd("multiple-clones",
                   doc(f("id", "1111"),
                       f("category", "misc"),
                       f("authors", "Isaac Asimov", "John Brunner"),
                       f("editors", "John W. Campbell"),
                       f("store1_price", 87),
                       f("store2_price", 78),
                       f("store3_price", (Object) null),
                       f("list_price", 1000),
                       f("features", "Pages!", "Binding!"),
                       f("feat_of_strengths", "Pullups")));
                       
    assertNotNull(d);
    assertEquals("misc", d.getFieldValue("category"));
    assertEquals("misc", d.getFieldValue("category_s"));
    assertEquals(Arrays.asList("Isaac Asimov", "John Brunner"),
                 d.getFieldValues("authors"));
    assertEquals(Collections.singletonList("John W. Campbell"),
                 d.getFieldValues("editors"));
    assertEquals(Arrays.asList("Isaac Asimov", "John Brunner",
                               "John W. Campbell"),
                 d.getFieldValues("contributors"));
    assertEquals(87, d.getFieldValue("store1_price"));
    assertEquals(78, d.getFieldValue("store2_price"));
    assertEquals(1000, d.getFieldValue("list_price"));
    assertEquals(Arrays.asList(87, 78),
                 d.getFieldValues("all_prices"));
    
    assertEquals(Arrays.asList("Pages!", "Binding!"),
                 d.getFieldValues("key_feature"));
    assertEquals("Pullups", d.getFieldValue("key_feat_of_strength"));
  }

  public void testCloneCombinations() throws Exception {

    SolrInputDocument d;

    // maxChars
    d = processAdd("clone-max-chars",
                   doc(f("id", "1111"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text", d.getFieldValue("field1"));
    assertEquals("tex", d.getFieldValue("toField"));

    // move
    d = processAdd("clone-move",
                   doc(f("id", "1111"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text", d.getFieldValue("toField"));
    assertFalse(d.containsKey("field1"));

    // replace
    d = processAdd("clone-replace",
                   doc(f("id", "1111"),
                       f("toField", "IGNORED"),
                       f("field1", "text")));
    assertNotNull(d);
    assertEquals("text", d.getFieldValue("field1"));
    assertEquals("text", d.getFieldValue("toField"));

    // append
    d = processAdd("clone-append",
                   doc(f("id", "1111"),
                       f("toField", "aaa"),
                       f("field1", "bbb"),
                       f("field2", "ccc")));
    assertNotNull(d);
    assertEquals("bbb", d.getFieldValue("field1"));
    assertEquals("ccc", d.getFieldValue("field2"));
    assertEquals("aaa; bbb; ccc", d.getFieldValue("toField"));

    // first value
    d = processAdd("clone-first",
                   doc(f("id", "1111"),
                       f("field0", "aaa"),
                       f("field1", "bbb"),
                       f("field2", "ccc")));
    assertNotNull(d);
    assertEquals("aaa", d.getFieldValue("toField"));
  }
  
}
