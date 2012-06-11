package org.apache.lucene.facet.index.params;

import org.apache.lucene.index.Term;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.search.DrillDown;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.util.PartitionsUtils;

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

public class PerDimensionIndexingParamsTest extends LuceneTestCase {

  @Test
  public void testTopLevelSettings() {
    FacetIndexingParams ifip = new PerDimensionIndexingParams();
    assertNotNull("Missing default category list", ifip
        .getAllCategoryListParams());
    assertEquals(
        "Expected default category list term is $facets:$fulltree$",
        new Term("$facets", "$fulltree$"), ifip.getCategoryListParams(
            null).getTerm());
    String expectedDDText = "a"
        + ifip.getFacetDelimChar() + "b";
    CategoryPath cp = new CategoryPath("a", "b");
    assertEquals("wrong drill-down term", new Term("$facets",
        expectedDDText), DrillDown.term(ifip,cp));
    char[] buf = new char[20];
    int numchars = ifip.drillDownTermText(cp, buf);
    assertEquals("3 characters should be written", 3, numchars);
    assertEquals("wrong drill-down term text", expectedDDText, new String(
        buf, 0, numchars));
    
    CategoryListParams clParams = ifip.getCategoryListParams(null);
    assertEquals("partition for all ordinals is the first", "$fulltree$", 
        PartitionsUtils.partitionNameByOrdinal(ifip, clParams , 250));
    assertEquals("for partition 0, the same name should be returned",
        "$fulltree$", PartitionsUtils.partitionName(clParams, 0));
    assertEquals(
        "for any other, it's the concatenation of name + partition",
        "$fulltree$1", PartitionsUtils.partitionName(clParams, 1));
    assertEquals("default partition number is always 0", 0, 
        PartitionsUtils.partitionNumber(ifip,100));
    
    assertEquals("default partition size is unbounded", Integer.MAX_VALUE,
        ifip.getPartitionSize());
  }

  @Test
  public void testCategoryListParamsAddition() {
    PerDimensionIndexingParams tlfip = new PerDimensionIndexingParams();
    CategoryListParams clp = new CategoryListParams(
        new Term("clp", "value"));
    tlfip.addCategoryListParams(new CategoryPath("a"), clp);
    assertEquals("Expected category list term is " + clp.getTerm(), clp
        .getTerm(), tlfip.getCategoryListParams(new CategoryPath("a"))
        .getTerm());
    assertNotSame("Unexpected default category list " + clp.getTerm(), clp,
        tlfip.getCategoryListParams(null));
  }

}