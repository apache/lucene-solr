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
package org.apache.solr.search.join;

import java.util.Map;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SortSpecParsing;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNestedDocsSort extends SolrTestCaseJ4 {
    
    @BeforeClass
    public static void beforeClass() throws Exception {
      initCore("solrconfig.xml", "schema.xml");
    }
    
    public void testEquality(){
      parseAssertEq("childfield(name_s1,$q) asc", "childfield(name_s1,$q) asc");
      parseAssertEq("childfield(name_s1,$q) asc", "childfield(name_s1) asc");  
      parseAssertEq("childfield(name_s1,$q) asc", "childfield(name_s1,) asc");   
      
      parseAssertNe("childfield(name_s1,$q) asc", "childfield(name_s1,$q) desc");
      parseAssertNe("childfield(name_s1,$q) asc", "childfield(surname_s1,$q) asc");
      parseAssertNe("childfield(name_s1,$q) asc", "childfield(surname_s1,$q2) desc");
    }
    
    public void testEqualityUpToBlockJoin(){
      parseAssertNe("childfield(name_s1,$q) asc","childfield(name_s1,$q2) asc");
    }
    
    @Test(expected=SolrException.class)
    public void testNotBjqReference(){
      parse("childfield(name_s1,$notbjq) asc");
    }
    
    // root cause is swallowed, but it's logged there. 
    @Test(expected=SolrException.class)
    public void testOmitFieldWithComma(){
      parse("childfield(,$q)  asc");
    }
    @Test(expected=SolrException.class)
    public void testOmitField(){
      parse("childfield($q)  asc");
    }

    @Test(expected=SolrException.class)
    public void testForgetEverything(){
      parse("childfield() asc");
    }
    
    @Test(expected=SolrException.class)
    public void testEvenBraces(){
      parse("childfield asc");
    }
    
    @Test(expected=SolrException.class)
    public void testAbsentField(){
      parse("childfield(NEVER_SEEN_IT,$q) asc");
    }
    
    @Test(expected=SolrException.class)
    public void testOmitOrder(){
      parse("childfield(name_s1,$q)");
    }
    
    @Test
    public void testOmitSpaceinFrontOfOrd(){
      parseAssertEq("childfield(name_s1,$q)asc", "childfield(name_s1,$q) asc");
    }
    
    private void parseAssertEq(String sortField, String sortField2) {
      assertEq(parse(sortField), parse(sortField2));
    }
    
    private void assertEq(SortField sortField, SortField sortField2) {
      assertEquals(sortField, sortField2);
      assertEquals(sortField.hashCode(), sortField2.hashCode());
    }
    
    private void parseAssertNe(String sortField, String sortField2) {
      assertFalse(parse(sortField).equals(parse(sortField2)));
    }

    private SortField parse(String a) {
        final SolrQueryRequest req = req("q", "{!parent which=type_s1:parent}whatever_s1:foo",
            "q2", "{!parent which=type_s1:parent}nomater_s1:what",
            "notbjq", "foo_s1:bar");
        try {
        final SortSpec spec = SortSpecParsing.parseSortSpec(a,
            req);
        assertNull(spec.getSchemaFields().get(0));
        final Sort sort = spec.getSort();
        final SortField field = sort.getSort()[0];
        assertNotNull(field);
        return field;
      } finally {
        req.close();
      }
    }

    public void testCachehits(){
      final SolrQueryRequest req = req();
      try {
        final SolrCache cache = req.getSearcher().getCache("perSegFilter");
        assertNotNull(cache);
        final Map<String,Object> state = cache.getSolrMetricsContext().getMetricsSnapshot();
        String lookupsKey = null;
        for(String key : state.keySet()){
          if(key.endsWith(".lookups")) {
            lookupsKey = key;
            break;
          }
        }
        Number before = (Number) state.get(lookupsKey);
        parse("childfield(name_s1,$q) asc");
        Number after = (Number) cache.getSolrMetricsContext().getMetricsSnapshot().get(lookupsKey);
        assertEquals("parsing bjq lookups parent filter,"
            + "parsing sort spec lookups parent and child filters, "
            + "hopefully for the purpose",3, after.intValue()-before.intValue());
      } finally {
        req.close();
      }
    }
    
    

    
}
