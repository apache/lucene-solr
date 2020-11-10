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

package org.apache.solr.analytics.util;

import org.apache.solr.analytics.legacy.facet.LegacyAbstractAnalyticsFacetTest;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsExpressionSortRequest;
import static org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsRequest;
import static org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsValueFacetRequest;

import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsFacetRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsRangeFacetRequest;

public class OldAnalyticsRequestConverterUnitTest extends LegacyAbstractAnalyticsFacetTest {
  String fileName = "facetWithDottedFields.txt";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml", "schema-analytics.xml");
  }

  @Test
  public void testConvertFieldFacetWithDottedField() throws Exception {
    SolrParams params = request(fileToStringArr(OldAnalyticsRequestConverterUnitTest.class, fileName)).getParams();
    AnalyticsRequest request = OldAnalyticsRequestConverter.convert(params);
    final AnalyticsValueFacetRequest analyticsValueFacetRequest = (AnalyticsValueFacetRequest) request.groupings.get("df1").facets.get("long.dotfield");
    assertNotNull("Sort param should be parsed for dotted field", analyticsValueFacetRequest.sort);
    final AnalyticsExpressionSortRequest analyticsExpressionSortRequest = (AnalyticsExpressionSortRequest) analyticsValueFacetRequest.sort.criteria.get(0);
    assertEquals("Sort param expression should be parsed for dotted field",
        "mean", analyticsExpressionSortRequest.expression);
    assertEquals("Sort param direction should be parsed for dotted field",
        "asc", analyticsExpressionSortRequest.direction);
  }

  @Test
  public void testConvertRangeFacetWithDottedField() throws Exception {
    SolrParams params = request(fileToStringArr(OldAnalyticsRequestConverterUnitTest.class, fileName)).getParams();
    AnalyticsRequest request = OldAnalyticsRequestConverter.convert(params);

    final AnalyticsFacetRequest analyticsFacetRequest = request.groupings.get("df2").facets.get("long.dotfield");
    assertNotNull("Range facet param should be parsed for dotted field", analyticsFacetRequest);
    assertEquals("30", ((AnalyticsRangeFacetRequest)analyticsFacetRequest).end);
    assertEquals(true, ((AnalyticsRangeFacetRequest)analyticsFacetRequest).hardend);
    
  }
}
