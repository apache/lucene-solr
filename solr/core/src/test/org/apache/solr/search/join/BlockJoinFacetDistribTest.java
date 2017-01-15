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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockJoinFacetDistribTest extends SolrCloudTestCase{

  private static final String collection = "facetcollection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    String configName = "solrCloudCollectionConfig";
    int nodeCount = 6;
    configureCluster(nodeCount)
       .addConfig(configName, configDir)
       .configure();
    
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-blockjoinfacetcomponent.xml" );
    collectionProperties.put("schema", "schema-blockjoinfacetcomponent.xml"); 
    
    // create a collection holding data for the "to" side of the JOIN
    
    int shards = 3;
    int replicas = 2 ;
    CollectionAdminRequest.createCollection(collection, configName, shards, replicas)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

  }

  final static List<String> colors = Arrays.asList("red","blue","brown","white","black","yellow","cyan","magenta","blur",
      "fuchsia", "light","dark","green","grey","don't","know","any","more" );
  final static List<String> sizes = Arrays.asList("s","m","l","xl","xxl","xml","xxxl","3","4","5","6","petite","maxi");
  
  @Test
  public void testBJQFacetComponent() throws Exception {
    
    assert ! colors.removeAll(sizes): "there is no colors in sizes";
    Collections.shuffle(colors,random());
    List<String> matchingColors = colors.subList(0, Math.min(atLeast(random(), 2), colors.size()));
        
    Map<String, Set<Integer>> parentIdsByAttrValue = new HashMap<String, Set<Integer>>(){
      @Override
      public Set<Integer> get(Object key) {
        return super.get(key)==null && put((String)key, new HashSet<>())==null?super.get(key):super.get(key);
      }
    };
    
    cluster.getSolrClient().deleteByQuery(collection, "*:*");
    
    final int parents = atLeast(10);
    boolean aggregationOccurs = false;
    List<SolrInputDocument> parentDocs = new ArrayList<>();
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
      parentDocs.add(pdoc);
      if (!parentDocs.isEmpty() && rarely()) {
        indexDocs(parentDocs);
        parentDocs.clear();
        cluster.getSolrClient().commit(collection, false, false, true);
      }
    }
    if (!parentDocs.isEmpty()) {
      indexDocs(parentDocs);
    }
    cluster.getSolrClient().commit(collection);

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
          assertEquals(c.getName()+"("+msg+")", 
              parentIdsByAttrValue.get(c.getName()).size(), c.getCount());
        }
      }
      
      assertEquals(msg , parentIdsByAttrValue.size(),color_s.getValueCount() + size_s.getValueCount());
      //System.out.println(parentIdsByAttrValue);
  }

  private QueryResponse query(String ... arg) throws SolrServerException, IOException {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for(int i=0; i<arg.length; i+=2) {
      solrParams.add(arg[i], arg[i+1]);
    }
    return cluster.getSolrClient().query(collection, solrParams);
  }

  private void indexDocs(Collection<SolrInputDocument> pdocs) throws SolrServerException, IOException {
    cluster.getSolrClient().add(collection, pdocs);
  }
}
