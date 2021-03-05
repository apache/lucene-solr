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
package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.function.FieldNameValueSource;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.noggit.ObjectBuilder;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/** Whitebox test of the various syntaxes for specifying stats in JSON Facets */
public class TestJsonFacetsStatsParsing extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  public void testSortEquality() throws Exception {
    assertEquals(new FacetRequest.FacetSort("count", FacetRequest.SortDirection.desc),
                 FacetRequest.FacetSort.COUNT_DESC);
    assertEquals(new FacetRequest.FacetSort("index", FacetRequest.SortDirection.asc),
                 FacetRequest.FacetSort.INDEX_ASC);
    assertEquals(new FacetRequest.FacetSort("foo", FacetRequest.SortDirection.asc),
                 new FacetRequest.FacetSort("foo", FacetRequest.SortDirection.asc));
    // negative assertions...
    assertThat(new FacetRequest.FacetSort("foo", FacetRequest.SortDirection.desc),
               not(new FacetRequest.FacetSort("foo", FacetRequest.SortDirection.asc)));
    assertThat(new FacetRequest.FacetSort("bar", FacetRequest.SortDirection.desc),
               not(new FacetRequest.FacetSort("foo", FacetRequest.SortDirection.desc)));
  }
  
  public void testEquality() throws IOException {
    try (SolrQueryRequest req = req("custom_req_param","foo_i",
                                    "overridden_param","xxxxx_i")) {
      
      @SuppressWarnings({"unchecked"})
      final FacetRequest fr = FacetRequest.parse
        (req, (Map<String,Object>) Utils.fromJSONString
         ("{ " +
             // with valuesource
             "  f1:'min(field(\"foo_i\"))', " +
             "  f2:'min(field($custom_req_param))', " +
             // with fieldName and query de-reference
             "  s1:'min(foo_i)', " +
             "  s2:'min($custom_req_param)', " +
             "  s3:{ func:'min($custom_req_param)' }, " +
             "  s4:{ type:func, func:'min($custom_req_param)' }, " +
             "  s5:{ type:func, func:'min($custom_local_param)', custom_local_param:foo_i }, " +
             "  s6:{ type:func, func:'min($overridden_param)', overridden_param:foo_i }, " +
             // test the test...
             "  diff:'min(field(\"bar_i\"))'," +
             "}"));
         
      final Map<String, AggValueSource> stats = fr.getFacetStats();
      assertEquals(9, stats.size());
      
      for (Map.Entry<String,AggValueSource> entry : stats.entrySet()) {
        final String key = entry.getKey();
        final AggValueSource agg = entry.getValue();
        
        assertEquals("name of " + key, "min", agg.name());
        MatcherAssert.assertThat("type of " + key, agg, instanceOf(SimpleAggValueSource.class));
        SimpleAggValueSource sagg = (SimpleAggValueSource) agg;

        if (key.startsWith("f")) { // value source as arg to min
          MatcherAssert.assertThat("vs of " + key, sagg.getArg(), instanceOf(IntFieldSource.class));
          assertEquals("field of " + key, "foo_i", ((IntFieldSource)sagg.getArg()).getField());
          assertEquals(key + ".equals(f1)", agg, stats.get("f1"));
        } else if (key.startsWith("s")) { // field as arg to min
          MatcherAssert.assertThat("vs of " + key, sagg.getArg(), instanceOf(FieldNameValueSource.class));
          assertEquals("field of " + key, "foo_i", ((FieldNameValueSource)sagg.getArg()).getFieldName());
          assertEquals(key + ".equals(s1)", agg, stats.get("s1"));
          assertEquals("s1.equals("+key+")", stats.get("s1"), agg);
        } else if ("diff".equals(key)) {
          assertEquals("field of " + key, "bar_i", ((IntFieldSource)sagg.getArg()).getField());
          assertNotEquals("diff.equals(s1) ?!?!", agg, stats.get("f1"));
        }
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  public void testVerboseSyntaxWithLocalParams() throws IOException {
    // some parsers may choose to use "global" req params as defaults/shadows for
    // local params, but DebugAgg does not -- so use these to test that the
    // JSON Parsing doesn't pollute the local params the ValueSourceParser gets...
    try (SolrQueryRequest req = req("foo", "zzzz", "yaz", "zzzzz")) { 
      final FacetRequest fr = FacetRequest.parse
        (req, (Map<String,Object>) ObjectBuilder.fromJSON
         ("{ x:{type:func, func:'debug()', foo:['abc','xyz'], bar:4.2 } }"));

      final Map<String, AggValueSource> stats = fr.getFacetStats();
      assertEquals(1, stats.size());
      AggValueSource agg = stats.get("x");
      assertNotNull(agg);
      MatcherAssert.assertThat(agg, instanceOf(DebugAgg.class));
      
      DebugAgg x = (DebugAgg)agg;
      assertEquals(new String[] {"abc", "xyz"}, x.localParams.getParams("foo"));
      assertEquals((Float)4.2F, x.localParams.getFloat("bar"));
      assertNull(x.localParams.get("yaz"));
    }
  }
}
