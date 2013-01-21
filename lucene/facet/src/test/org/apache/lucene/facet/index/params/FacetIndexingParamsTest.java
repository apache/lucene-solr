package org.apache.lucene.facet.index.params;

import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
import org.apache.lucene.facet.search.DrillDown;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.index.Term;
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

public class FacetIndexingParamsTest extends LuceneTestCase {

  @Test
  public void testDefaultSettings() {
    FacetIndexingParams dfip = FacetIndexingParams.ALL_PARENTS;
    assertNotNull("Missing default category list", dfip.getAllCategoryListParams());
    assertEquals("all categories have the same CategoryListParams by default",
        dfip.getCategoryListParams(null), dfip.getCategoryListParams(new CategoryPath("a")));
    assertEquals("Expected default category list field is $facets", "$facets", dfip.getCategoryListParams(null).field);
    String expectedDDText = "a"
        + dfip.getFacetDelimChar() + "b";
    CategoryPath cp = new CategoryPath("a", "b");
    assertEquals("wrong drill-down term", new Term("$facets",
        expectedDDText), DrillDown.term(dfip,cp));
    char[] buf = new char[20];
    int numchars = dfip.drillDownTermText(cp, buf);
    assertEquals("3 characters should be written", 3, numchars);
    assertEquals("wrong drill-down term text", expectedDDText, new String(
        buf, 0, numchars));
    assertEquals("partition for all ordinals is the first", "", 
        PartitionsUtils.partitionNameByOrdinal(dfip, 250));
    assertEquals("for partition 0, the same name should be returned",
        "", PartitionsUtils.partitionName(0));
    assertEquals(
        "for any other, it's the concatenation of name + partition",
        PartitionsUtils.PART_NAME_PREFIX + "1", PartitionsUtils.partitionName(1));
    assertEquals("default partition number is always 0", 0, 
        PartitionsUtils.partitionNumber(dfip,100));
    assertEquals("default partition size is unbounded", Integer.MAX_VALUE,
        dfip.getPartitionSize());
  }

  @Test
  public void testCategoryListParamsWithDefaultIndexingParams() {
    CategoryListParams clp = new CategoryListParams("clp");
    FacetIndexingParams dfip = new FacetIndexingParams(clp);
    assertEquals("Expected default category list field is " + clp.field, clp.field, dfip.getCategoryListParams(null).field);
  }

  @Test
  public void testCategoryPolicies() {
    FacetIndexingParams dfip = FacetIndexingParams.ALL_PARENTS;
    // check path policy
    CategoryPath cp = CategoryPath.EMPTY;
    PathPolicy pathPolicy = PathPolicy.ALL_CATEGORIES;
    assertEquals("path policy does not match default for root", pathPolicy.shouldAdd(cp), dfip.getPathPolicy().shouldAdd(cp));
    for (int i = 0; i < 30; i++) {
      int nComponents = random().nextInt(10) + 1;
      String[] components = new String[nComponents];
      for (int j = 0; j < components.length; j++) {
        components[j] = (Integer.valueOf(random().nextInt(30))).toString();
      }
      cp = new CategoryPath(components);
      assertEquals("path policy does not match default for " + cp.toString('/'), 
          pathPolicy.shouldAdd(cp), dfip.getPathPolicy().shouldAdd(cp));
    }

    // check ordinal policy
    OrdinalPolicy ordinalPolicy = OrdinalPolicy.ALL_PARENTS;
    assertEquals("ordinal policy does not match default for root", 
        ordinalPolicy.shouldAdd(TaxonomyReader.ROOT_ORDINAL), 
        dfip.getOrdinalPolicy().shouldAdd(TaxonomyReader.ROOT_ORDINAL));
    for (int i = 0; i < 30; i++) {
      int ordinal = random().nextInt();
      assertEquals("ordinal policy does not match default for " + ordinal, 
          ordinalPolicy.shouldAdd(ordinal),
          dfip.getOrdinalPolicy().shouldAdd(ordinal));
    }
  }

}