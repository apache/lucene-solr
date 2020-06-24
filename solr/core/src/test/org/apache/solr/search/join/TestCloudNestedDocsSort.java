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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloudNestedDocsSort extends SolrCloudTestCase {

  private static ArrayList<String> vals = new ArrayList<>();
  private static CloudSolrClient client;
  private static int maxDocs;
  private static String matchingParent;
  private static String matchingChild;

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int numVals = atLeast(10);
    for (int i=0; i < numVals; i++) {
      vals.add(""+Integer.toString(random().nextInt(1000000), Character.MAX_RADIX));
    }
    
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    String configName = "solrCloudCollectionConfig";
    int nodeCount = 5;
    configureCluster(nodeCount)
       .addConfig(configName, configDir)
       .configure();
    
    int shards = 2;
    int replicas = 2 ;
    CollectionAdminRequest.createCollection("collection1", configName, shards, replicas)
        .withProperty("config", "solrconfig-minimal.xml")
        .withProperty("schema", "schema.xml")
        .process(cluster.getSolrClient());

    client = cluster.getSolrClient();
    client.setDefaultCollection("collection1");
    
    ZkStateReader zkStateReader = client.getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("collection1", zkStateReader, true, true, 30);
    
    {
      int id = 42;
      final List<SolrInputDocument> docs = new ArrayList<>();
      final int parentsNum = atLeast(20);
          ;
      for (int i=0; i<parentsNum || (matchingParent==null ||matchingChild==null); i++) {
        final String parentTieVal = "" + random().nextInt(5);
        final String parentId = ""+(id++);
        final SolrInputDocument parent = new SolrInputDocument("id", parentId,
            "type_s", "parent",
            "parentTie_s1", parentTieVal,
            "parent_id_s1", parentId
            );
        final List<String> parentFilter = addValsField(parent, "parentFilter_s");
        final int kids = usually() ? atLeast(20) : 0;
        for(int c = 0; c< kids; c++){
          SolrInputDocument child = new SolrInputDocument("id", ""+(id++),
              "type_s", "child",
              "parentTie_s1", parentTieVal,
              "parent_id_s1", parentId);
          child.addField("parentFilter_s", parentFilter);
          if (usually()) {
            child.addField( "val_s1", Integer.toString(random().nextInt(1000), Character.MAX_RADIX)+"" );
          }
          final List<String> chVals = addValsField(child, "childFilter_s");
          parent.addChildDocument(child );

          // let's pickup at least matching child
          final boolean canPickMatchingChild = !chVals.isEmpty() && !parentFilter.isEmpty();
          final boolean haveNtPickedMatchingChild = matchingParent==null ||matchingChild==null;
          if (canPickMatchingChild && haveNtPickedMatchingChild && usually()) {
            matchingParent = parentFilter.iterator().next();
            matchingChild = chVals.iterator().next();
          }
        }
        maxDocs += parent.getChildDocumentCount()+1;
        docs.add(parent);
      }
      // don't add parents in increasing uniqueKey order
      Collections.shuffle(docs, random());
      client.add(docs);
      client.commit();
    }
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    client = null;
  }

  @Test 
  public void test() throws SolrServerException, IOException {
    final boolean asc = random().nextBoolean();
    final String dir = asc ? "asc": "desc";
    final String parentFilter = "+parentFilter_s:("+matchingParent+" "+anyValsSpaceDelim(2)+")^=0";
    String childFilter = "+childFilter_s:("+matchingChild+" "+anyValsSpaceDelim(4)+")^=0";
    final String fl = "id,type_s,parent_id_s1,val_s1,score,parentFilter_s,childFilter_s,parentTie_s1";
    String sortClause = "val_s1 "+dir+", "+"parent_id_s1 "+ascDesc();
    if(rarely()) {
      sortClause ="parentTie_s1 "+ascDesc()+","+sortClause;
    }
    final SolrQuery q = new SolrQuery("q", "+type_s:child^=0 "+parentFilter+" "+
          childFilter ,
        "sort", sortClause, 
        "rows", ""+maxDocs,
        "fl",fl);

    final QueryResponse children = client.query(q);
    
    final SolrQuery bjq = random().nextBoolean() ? 
         new SolrQuery(// top level bjq
           "q", "{!parent which=type_s:parent}(+type_s:child^=0 "+parentFilter+" "+ childFilter+")",
           "sort", sortClause.replace("val_s1", "childfield(val_s1)"),
           "rows", ""+maxDocs, "fl", fl)
         :
        new SolrQuery(// same bjq as a subordinate clause
           "q", "+type_s:parent "+parentFilter+" +{!v=$parentcaluse}",
           "parentcaluse","{!parent which=type_s:parent v='"+(childFilter).replace("+", "")+"'}",
           "sort", sortClause.replace("val_s1", "childfield(val_s1,$parentcaluse)"),
           "rows", ""+maxDocs, "fl", fl);

    final QueryResponse parents = client.query(bjq);
    
    Set<String> parentIds = new LinkedHashSet<>();
    assertTrue("it can never be empty for sure", parents.getResults().size()>0);
    for(Iterator<SolrDocument> parentIter = parents.getResults().iterator(); parentIter.hasNext();) {
      for (SolrDocument child : children.getResults()) {
        assertEquals("child", child.getFirstValue("type_s"));
        final String parentId = (String) child.getFirstValue("parent_id_s1");
        if( parentIds.add(parentId) ) { // in children the next parent appears, it should be next at parents 
          final SolrDocument parent = parentIter.next();
          assertEquals("parent", parent.getFirstValue("type_s"));
          final String actParentId = ""+ parent.get("id");
          if (!actParentId.equals(parentId)) {
            final String chDump = children.toString().replace("SolrDocument","\nSolrDocument");
            System.out.println("\n\n"+chDump+"\n\n");
            System.out.println("\n\n"+parents.toString().replace("SolrDocument","\nSolrDocument")
                +"\n\n");
          }
          assertEquals(""+child+"\n"+parent,actParentId, parentId);
        }
      }
    }

    
  }

  private String ascDesc() {
    return random().nextBoolean() ? "asc": "desc";
  }

  protected String anyValsSpaceDelim(int howMany) {
    Collections.shuffle(vals, random());
    return vals.subList(0, howMany).toString().replaceAll("[,\\[\\]]", "");
  }

  protected static List<String> addValsField(final SolrInputDocument parent, final String field) {
    Collections.shuffle(vals, random());
    final ArrayList<String> values = new ArrayList<>(vals.subList(0, 1+random().nextInt(vals.size()-1)));
    assertFalse(values.isEmpty());
    parent.addField(field, values);
    return values;
  }
  
}
