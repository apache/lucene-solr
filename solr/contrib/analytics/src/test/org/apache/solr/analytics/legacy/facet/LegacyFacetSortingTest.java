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
package org.apache.solr.analytics.legacy.facet;

import org.apache.solr.analytics.legacy.LegacyAbstractAnalyticsTest;
import org.apache.solr.analytics.legacy.expression.LegacyExpressionTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyFacetSortingTest extends LegacyAbstractAnalyticsTest {
  private static String fileName = "facetSorting.txt";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml", "schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    // The data set below is so generated that in bucket corresponding fieldFacet B, double_dd column has null values
    // and in bucket C corresponding to fieldFacet C has null values for column long_ld.
    // FieldFaceting occurs on string_sd field
    assertU(adoc("id", "1001", "string_sd", "A", "double_dd", "" + 3, "long_ld", "" + 1));
    assertU(adoc("id", "1002", "string_sd", "A", "double_dd", "" + 25, "long_ld", "" + 2));
    assertU(adoc("id", "1003", "string_sd", "B", "long_ld", "" + 3));
    assertU(adoc("id", "1004", "string_sd", "B", "long_ld", "" + 4));
    assertU(adoc("id", "1005", "string_sd", "C",                       "double_dd", "" + 17));

    assertU(commit());
    String response = h.query(request(fileToStringArr(LegacyExpressionTest.class, fileName)));
    setResponse(response);
  }

  @Test
  public void addTest() throws Exception {
    Double minResult = (Double) getStatResult("ar", "min", VAL_TYPE.DOUBLE);
    Long maxResult = (Long) getStatResult("ar", "max", VAL_TYPE.LONG);
    assertEquals(Double.valueOf(minResult), Double.valueOf(3.0));
    assertEquals(Long.valueOf(maxResult),Long.valueOf(4));
  }
}
