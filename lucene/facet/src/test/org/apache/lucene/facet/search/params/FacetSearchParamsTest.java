package org.apache.lucene.facet.search.params;

import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class FacetSearchParamsTest extends LuceneTestCase {

  @Test
  public void testAddFacetRequest() throws Exception {
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("a", "b"), 1));
    assertEquals("expected 1 facet request", 1, fsp.getFacetRequests().size());
  }
  
  @Test
  public void testPartitionSizeWithCategories() throws Exception {
    Directory dir = newDirectory();
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dir);
    tw.addCategory(new CategoryPath("a"));
    tw.commit();
    tw.close();
    TaxonomyReader tr = new DirectoryTaxonomyReader(dir);
    assertEquals("unexpected partition offset for 1 categories", 2,
        PartitionsUtils.partitionOffset(FacetIndexingParams.ALL_PARENTS, 1, tr));
    assertEquals("unexpected partition size for 1 categories", 2,
        PartitionsUtils.partitionSize(FacetIndexingParams.ALL_PARENTS,tr));
    tr.close();
    dir.close();
  }
  
  @Test
  public void testSearchParamsWithNullRequest() throws Exception {
    try {
      assertNull(new FacetSearchParams());
      fail("FacetSearchParams should throw IllegalArgumentException when not adding requests");
    } catch (IllegalArgumentException e) {
    }
  }
}
