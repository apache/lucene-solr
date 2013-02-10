package org.apache.lucene.facet.params;

import java.util.Collections;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.index.Term;
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

public class PerDimensionIndexingParamsTest extends FacetTestCase {

  @Test
  public void testTopLevelSettings() {
    FacetIndexingParams ifip = new PerDimensionIndexingParams(Collections.<CategoryPath, CategoryListParams>emptyMap());
    assertNotNull("Missing default category list", ifip.getAllCategoryListParams());
    assertEquals("Expected default category list field is $facets", "$facets", ifip.getCategoryListParams(null).field);
    String expectedDDText = "a" + ifip.getFacetDelimChar() + "b";
    CategoryPath cp = new CategoryPath("a", "b");
    assertEquals("wrong drill-down term", new Term("$facets", expectedDDText), DrillDownQuery.term(ifip,cp));
    char[] buf = new char[20];
    int numchars = ifip.drillDownTermText(cp, buf);
    assertEquals("3 characters should be written", 3, numchars);
    assertEquals("wrong drill-down term text", expectedDDText, new String(buf, 0, numchars));
    
    assertEquals("partition for all ordinals is the first", "", PartitionsUtils.partitionNameByOrdinal(ifip, 250));
    assertEquals("for partition 0, the same name should be returned", "", PartitionsUtils.partitionName(0));
    assertEquals("for any other, it's the concatenation of name + partition", PartitionsUtils.PART_NAME_PREFIX + "1", PartitionsUtils.partitionName(1));
    assertEquals("default partition number is always 0", 0, PartitionsUtils.partitionNumber(ifip,100));
    assertEquals("default partition size is unbounded", Integer.MAX_VALUE, ifip.getPartitionSize());
  }

  @Test
  public void testCategoryListParamsAddition() {
    CategoryListParams clp = new CategoryListParams("clp");
    PerDimensionIndexingParams tlfip = new PerDimensionIndexingParams(
        Collections.<CategoryPath,CategoryListParams> singletonMap(new CategoryPath("a"), clp));
    assertEquals("Expected category list field is " + clp.field, 
        clp.field, tlfip.getCategoryListParams(new CategoryPath("a")).field);
    assertNotSame("Unexpected default category list " + clp.field, clp, tlfip.getCategoryListParams(null));
  }

}