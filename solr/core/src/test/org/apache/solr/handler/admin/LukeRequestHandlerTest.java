/**
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

package org.apache.solr.handler.admin;

import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Arrays;

/**
 * :TODO: currently only tests some of the utilities in the LukeRequestHandler
 */
public class LukeRequestHandlerTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() {
    return "schema12.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void testHistogramBucket() {
    assertHistoBucket(0, 1);
    assertHistoBucket(1, 2);
    assertHistoBucket(2, 3);
    assertHistoBucket(2, 4);
    assertHistoBucket(3, 5);
    assertHistoBucket(3, 6);
    assertHistoBucket(3, 7);
    assertHistoBucket(3, 8);
    assertHistoBucket(4, 9);

    final int MAX_VALID = ((Integer.MAX_VALUE/2)+1)/2;

    assertHistoBucket(29,   MAX_VALID-1 );
    assertHistoBucket(29,   MAX_VALID   );
    assertHistoBucket(30, MAX_VALID+1 );

  }

  private void assertHistoBucket(int slot, int in) {
    assertEquals("histobucket: " + in, slot, 32 - Integer.numberOfLeadingZeros(Math.max(0, in - 1)));
  }

  @Test
  public void testLuke() {

    assertU(adoc("id","SOLR1000", "name","Apache Solr",
      "solr_si", "10",
      "solr_sl", "10",
      "solr_sf", "10",
      "solr_sd", "10",
      "solr_s", "10",
      "solr_sI", "10",
      "solr_sS", "10",
      "solr_t", "10",
      "solr_tt", "10",
      "solr_b", "true",
      "solr_i", "10",
      "solr_l", "10",
      "solr_f", "10",
      "solr_d", "10",
      "solr_ti", "10",
      "solr_tl", "10",
      "solr_tf", "10",
      "solr_td", "10",
      "solr_pi", "10",
      "solr_pl", "10",
      "solr_pf", "10",
      "solr_pd", "10",
      "solr_dt", "2000-01-01T01:01:01Z",
      "solr_tdt", "2000-01-01T01:01:01Z",
      "solr_pdt", "2000-01-01T01:01:01Z"
    ));
    assertU(commit());

    // test that Luke can handle all of the field types
    assertQ(req("qt","/admin/luke", "id","SOLR1000"));

    final int numFlags = EnumSet.allOf(FieldFlag.class).size();

    assertQ("Not all flags ("+numFlags+") mentioned in info->key",
            req("qt","/admin/luke"),
            numFlags+"=count(//lst[@name='info']/lst[@name='key']/str)");

    // code should be the same for all fields, but just in case do several
    for (String f : Arrays.asList("solr_t","solr_s","solr_ti",
                                  "solr_td","solr_pl","solr_dt","solr_b",
                                  "solr_sS","solr_sI")) {

      final String xp = getFieldXPathPrefix(f);
      assertQ("Not as many schema flags as expected ("+numFlags+") for " + f,
              req("qt","/admin/luke", "fl", f),
              numFlags+"=string-length("+xp+"[@name='schema'])");

    }

    // diff loop for checking 'index' flags,
    // only valid for fields that are indexed & stored
    for (String f : Arrays.asList("solr_t","solr_s","solr_ti",
                                  "solr_td","solr_pl","solr_dt","solr_b")) {

      final String xp = getFieldXPathPrefix(f);
      assertQ("Not as many index flags as expected ("+numFlags+") for " + f,
              req("qt","/admin/luke", "fl", f),
              numFlags+"=string-length("+xp+"[@name='index'])");

    final String hxp = getFieldXPathHistogram(f);
    assertQ("Historgram field should be present for field "+f,
        req("qt", "/admin/luke", "fl", f),
        hxp+"[@name='histogram']");
    }
  }

  private static String getFieldXPathHistogram(String field) {
    return "//lst[@name='fields']/lst[@name='"+field+"']/lst";
  }
  private static String getFieldXPathPrefix(String field) {
    return "//lst[@name='fields']/lst[@name='"+field+"']/str";
  }
  
}
