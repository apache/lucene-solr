package org.apache.lucene.facet.search.params;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;

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

public class FacetRequestTest extends LuceneTestCase {

  @Test(expected=IllegalArgumentException.class)
  public void testIllegalNumResults() throws Exception {
    new CountFacetRequest(new CategoryPath("a", "b"), 0);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testIllegalCategoryPath() throws Exception {
    new CountFacetRequest(null, 1);
  }

  @Test
  public void testHashAndEquals() {
    CountFacetRequest fr1 = new CountFacetRequest(new CategoryPath("a"), 8);
    CountFacetRequest fr2 = new CountFacetRequest(new CategoryPath("a"), 8);
    assertEquals("hashCode() should agree on both objects", fr1.hashCode(), fr2.hashCode());
    assertTrue("equals() should return true", fr1.equals(fr2));
    fr1.setDepth(10);
    assertFalse("equals() should return false as fr1.depth != fr2.depth", fr1.equals(fr2));
  }
  
  @Test
  public void testGetFacetResultHandlerDifferentTaxonomy() throws Exception {
    FacetRequest fr = new CountFacetRequest(new CategoryPath("a"), 10);
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    // create empty indexes, so that LTR ctor won't complain about a missing index.
    new IndexWriter(dir1, new IndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    TaxonomyReader tr1 = new DirectoryTaxonomyReader(dir1);
    TaxonomyReader tr2 = new DirectoryTaxonomyReader(dir2);
    FacetResultsHandler frh1 = fr.createFacetResultsHandler(tr1);
    FacetResultsHandler frh2 = fr.createFacetResultsHandler(tr2);
    assertTrue("should not return the same FacetResultHandler instance for different TaxonomyReader instances", frh1 != frh2);
    tr1.close();
    tr2.close();
    dir1.close();
    dir2.close();
  }
  
  @Test
  public void testImmutability() throws Exception {
    // Tests that after a FRH is created by FR, changes to FR are not reflected
    // in the FRH.
    FacetRequest fr = new CountFacetRequest(new CategoryPath("a"), 10);
    Directory dir = newDirectory();
    // create empty indexes, so that LTR ctor won't complain about a missing index.
    new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(dir);
    FacetResultsHandler frh = fr.createFacetResultsHandler(tr);
    fr.setDepth(10);
    assertEquals(FacetRequest.DEFAULT_DEPTH, frh.getFacetRequest().getDepth());
    tr.close();
    dir.close();
  }
  
  @Test
  public void testClone() throws Exception {
    FacetRequest fr = new CountFacetRequest(new CategoryPath("a"), 10);
    FacetRequest clone = fr.clone();
    fr.setDepth(10);
    assertEquals("depth should not have been affected in the clone", FacetRequest.DEFAULT_DEPTH, clone.getDepth());
  }
  
}
