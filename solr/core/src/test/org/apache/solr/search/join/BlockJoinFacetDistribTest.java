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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;

@Slow
public class BlockJoinFacetDistribTest extends BaseDistributedSearchTestCase {

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-blockjoinfacetcomponent.xml";
    configString = "solrconfig-blockjoinfacetcomponent.xml";
  }

  @ShardsFixed(num = 3)
  public void test() throws Exception {
    testBJQFacetComponent();
  }

  final static List<String> colors = Arrays.asList("red","blue","brown","white","black","yellow","cyan","magenta","blur",
      "fuchsia", "light","dark","green","grey","don't","know","any","more" );
  final static List<String> sizes = Arrays.asList("s","m","l","xl","xxl","xml","xxxl","3","4","5","6","petite","maxi");
  
  private void testBJQFacetComponent() throws Exception {
    
    assert ! colors.removeAll(sizes): "there is no colors in sizes";
    Collections.shuffle(colors,random());
    List<String> matchingColors = colors.subList(0, Math.min(atLeast(random(), 2), colors.size()));
        
    Map<String, Set<Integer>> parentIdsByAttrValue = new HashMap<String, Set<Integer>>(){
      @Override
      public Set<Integer> get(Object key) {
        return super.get(key)==null && put((String)key, new HashSet<>())==null?super.get(key):super.get(key);
      }
    };
    
    final int parents = atLeast(10);
    boolean aggregationOccurs = false;
    for(int parent=0; parent<parents || !aggregationOccurs;parent++){
      assert parent < 2000000 : "parent num "+parent+
           " aggregationOccurs:"+aggregationOccurs+". Sorry! too tricky loop condition.";
      SolrInputDocument pdoc = new SolrInputDocument();
      pdoc.addField("id", parent);
      pdoc.addField("type_s", "parent");
      final String parentBrand = "brand"+(random().nextInt(5));
      pdoc.addField("BRAND_s", parentBrand);
      
      for(int child=0; child<atLeast(colors.size()/2);child++){
        SolrInputDocument childDoc= new SolrInputDocument();
        final String color = colors.get(random().nextInt(colors.size()));
        childDoc.addField("COLOR_s", color);
        final String size = sizes.get(random().nextInt(sizes.size()));
        childDoc.addField("SIZE_s",  size);
        
        if(matchingColors.contains(color)){
          final boolean colorDupe = !parentIdsByAttrValue.get(color).add(parent);
          final boolean sizeDupe = !parentIdsByAttrValue.get(size).add(parent);
          aggregationOccurs |= colorDupe || sizeDupe;
        }
        pdoc.addChildDocument(childDoc);
      }
      indexDoc(pdoc);
    }
    commit();
    
    //handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIPVAL); // not a cloud test, but may use updateLog
    handle.put("maxScore", SKIP);// see org.apache.solr.TestDistributedSearch.test()
    handle.put("shards", SKIP);
    handle.put("distrib", SKIP);
    handle.put("rid", SKIP);
    handle.put("track", SKIP);
    handle.put("facet_fields", UNORDERED);
    handle.put("SIZE_s", UNORDERED);
    handle.put("COLOR_s", UNORDERED);
    
    // to parent query
    final String childQueryClause = "COLOR_s:("+(matchingColors.toString().replaceAll("[,\\[\\]]", " "))+")";
      QueryResponse results = query("q", "{!parent which=\"type_s:parent\"}"+childQueryClause,
          "facet", random().nextBoolean() ? "true":"false",
          "qt",  random().nextBoolean() ? "blockJoinDocSetFacetRH" : "blockJoinFacetRH",
          "child.facet.field", "COLOR_s",
          "child.facet.field", "SIZE_s",
          "rows","0" // we care only abt results 
          );
      NamedList<Object> resultsResponse = results.getResponse();
      assertNotNull(resultsResponse);
      FacetField color_s = results.getFacetField("COLOR_s");
      FacetField size_s = results.getFacetField("SIZE_s");
      
      String msg = ""+parentIdsByAttrValue+" "+color_s+" "+size_s;
      for (FacetField facet: new FacetField[]{color_s, size_s}) {
        for (Count c : facet.getValues()) {
          assertEquals(c.getName()+"("+msg+")", parentIdsByAttrValue.get(c.getName()).size(), c.getCount());
        }
      }
      
      assertEquals(msg , parentIdsByAttrValue.size(),color_s.getValueCount() + size_s.getValueCount());
  //  }
  }

  protected String getCloudSolrConfig() {
    return configString;
  }
}
