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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.highlight.SolrFragmentsBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

public class CustomHighlightComponentTest extends SolrCloudTestCase {

  public static class CustomHighlightComponent extends HighlightComponent {

    protected String id_key = "id";
    protected String snippets_key = "snippets";

    @Override
    protected String highlightingResponseField() {
      return "custom_highlighting";
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected Object convertHighlights(@SuppressWarnings({"rawtypes"})NamedList hl) {
      @SuppressWarnings({"rawtypes"})
      final ArrayList<SimpleOrderedMap> hlMaps = new ArrayList<>();
      for (int i=0; i<hl.size(); ++i) {
          @SuppressWarnings({"rawtypes"})
          SimpleOrderedMap hlMap = new SimpleOrderedMap<Object>();
          hlMap.add(id_key, hl.getName(i));
          hlMap.add(snippets_key, hl.getVal(i));
          hlMaps.add(hlMap);
      }
      return hlMaps;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Object[] newHighlightsArray(int size) {
      return new SimpleOrderedMap[size];
    }

    @Override
    protected void addHighlights(Object[] objArr, Object obj, Map<Object, ShardDoc> resultIds) {
      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap[] mapArr = (SimpleOrderedMap[])objArr;
      @SuppressWarnings({"unchecked", "rawtypes"})
      final ArrayList<SimpleOrderedMap> hlMaps = (ArrayList<SimpleOrderedMap>)obj;
      for (@SuppressWarnings({"rawtypes"})SimpleOrderedMap hlMap : hlMaps) {
        String id = (String)hlMap.get(id_key);
        ShardDoc sdoc = resultIds.get(id);
        int idx = sdoc.positionInResponse;
        mapArr[idx] = hlMap;
      }
    }

    @Override
    protected Object getAllHighlights(Object[] objArr) {
      @SuppressWarnings({"rawtypes"})
      final SimpleOrderedMap[] mapArr = (SimpleOrderedMap[])objArr;
      // remove nulls in case not all docs were able to be retrieved
      @SuppressWarnings({"rawtypes"})
      ArrayList<SimpleOrderedMap> mapList = new ArrayList<>();
      for (@SuppressWarnings({"rawtypes"})SimpleOrderedMap map : mapArr) {
        if (map != null) {
          mapList.add(map);
        }
      }
      return mapList;
    }

  }

  protected String customHighlightComponentClassName() {
    return CustomHighlightComponent.class.getName();
  }

  protected String id_key = "id";
  protected String snippets_key = "snippets";

  private static String COLLECTION;

  @BeforeClass
  public static void setupCluster() throws Exception {

    // decide collection name ...
    COLLECTION = "collection"+(1+random().nextInt(100)) ;
    // ... and shard/replica/node numbers
    final int numShards = 3;
    final int numReplicas = 2;
    final int maxShardsPerNode = 2;
    final int nodeCount = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

    // create and configure cluster
    configureCluster(nodeCount)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    // create an empty collection
    CollectionAdminRequest
    .createCollection(COLLECTION, "conf", numShards, numReplicas)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(maxShardsPerNode)
    .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void test() throws Exception {

    // determine custom search handler name (the exact name should not matter)
    final String customSearchHandlerName = "/custom_select"+random().nextInt();

    final String defaultHighlightComponentName = HighlightComponent.COMPONENT_NAME;
    final String highlightComponentName;

    // add custom component (if needed) and handler
    {
      if (random().nextBoolean()) {
        // default component
        highlightComponentName = defaultHighlightComponentName;
      } else {
        // custom component
        highlightComponentName = "customhighlight"+random().nextInt();
        cluster.getSolrClient().request(
            new ConfigRequest(
                "{\n" +
                "  'add-searchcomponent': {\n" +
                "    'name': '"+highlightComponentName+"',\n" +
                "    'class': '"+customHighlightComponentClassName()+"'\n" +
                "  }\n" +
                "}"),
            COLLECTION);
      }
      // handler
      cluster.getSolrClient().request(
          new ConfigRequest(
              "{\n" +
              "  'add-requesthandler': {\n" +
              "    'name' : '"+customSearchHandlerName+"',\n" +
              "    'class' : 'org.apache.solr.handler.component.SearchHandler',\n" +
              "    'components' : [ '"+QueryComponent.COMPONENT_NAME+"', '"+highlightComponentName+"' ]\n" +
              "  }\n" +
              "}"),
          COLLECTION);
    }

    // add some documents
    final String id = "id";
    final String t1 = "a_t";
    final String t2 = "b_t";
    {
      new UpdateRequest()
          .add(sdoc(id, 1, t1, "bumble bee", t2, "bumble bee"))
          .add(sdoc(id, 2, t1, "honey bee", t2, "honey bee"))
          .add(sdoc(id, 3, t1, "solitary bee", t2, "solitary bee"))
          .commit(cluster.getSolrClient(), COLLECTION);
    }

    // search for the documents
    {
      // compose the query
      final SolrQuery solrQuery =  new SolrQuery(t1+":bee");
      solrQuery.setRequestHandler(customSearchHandlerName);
      solrQuery.setHighlight(true);
      final boolean t1Highlights = random().nextBoolean();
      if (t1Highlights) {
        solrQuery.addHighlightField(t1);
      }
      final boolean t2Highlights = random().nextBoolean();
      if (t2Highlights) {
        solrQuery.addHighlightField(t2);
      }

      // make the query
      final QueryResponse queryResponse = new QueryRequest(solrQuery)
          .process(cluster.getSolrClient(), COLLECTION);

      // analyse the response
      final Map<String, Map<String, List<String>>> highlighting = queryResponse.getHighlighting();
      @SuppressWarnings({"unchecked"})
      final ArrayList<SimpleOrderedMap<Object>> custom_highlighting =
          (ArrayList<SimpleOrderedMap<Object>>)queryResponse.getResponse().get("custom_highlighting");

      if (defaultHighlightComponentName.equals(highlightComponentName)) {
        // regular 'highlighting' ...
        if (t1Highlights) {
          checkHighlightingResponseMap(highlighting, t1);
        }
        if (t2Highlights) {
          checkHighlightingResponseMap(highlighting, t2);
        }
        if (!t1Highlights && !t2Highlights) {
          checkHighlightingResponseMap(highlighting, null);
        }
        // ... and no 'custom_highlighting'
        assertNull(custom_highlighting);
      } else {
        // no regular 'highlighting' ...
        assertNull(highlighting);
        // ... but 'custom_highlighting'
        assertNotNull(custom_highlighting);
        if (t1Highlights) {
          checkHighlightingResponseList(custom_highlighting, t1);
        }
        if (t2Highlights) {
          checkHighlightingResponseList(custom_highlighting, t2);
        }
        if (!t1Highlights && !t2Highlights) {
          checkHighlightingResponseList(custom_highlighting, null);
        }
      }
    }
  }

  protected void checkHighlightingResponseMap(Map<String, Map<String, List<String>>> highlightingMap,
      String highlightedField) throws Exception {
    assertEquals("too few or too many keys: "+highlightingMap.keySet(),
        3, highlightingMap.size());
    checkHighlightingResponseMapElement(highlightingMap.get("1"), highlightedField, "bumble ", "bee");
    checkHighlightingResponseMapElement(highlightingMap.get("2"), highlightedField, "honey ", "bee");
    checkHighlightingResponseMapElement(highlightingMap.get("3"), highlightedField, "solitary ", "bee");
  }

  protected void checkHighlightingResponseMapElement(Map<String, List<String>> docHighlights,
      String highlightedField, String preHighlightText, String highlightedText) throws Exception {
    if (highlightedField == null) {
      assertEquals(0, docHighlights.size());
    } else {
      List<String> docHighlightsList = docHighlights.get(highlightedField);
      assertEquals(1, docHighlightsList.size());
      assertEquals(preHighlightText
          + SolrFragmentsBuilder.DEFAULT_PRE_TAGS
          + highlightedText
          + SolrFragmentsBuilder.DEFAULT_POST_TAGS, docHighlightsList.get(0));
    }
  }

  protected void checkHighlightingResponseList(ArrayList<SimpleOrderedMap<Object>> highlightingList,
      String highlightedField) throws Exception {
    assertEquals("too few or too many elements: "+highlightingList.size(),
        3, highlightingList.size());
    final Set<String> seenDocIds = new HashSet<>();
    for (SimpleOrderedMap<Object> highlightingListElementMap : highlightingList) {
      final String expectedHighlightText;
      final String actualHighlightText;
      // two elements in total: id and snippets
      assertEquals(highlightingList.toString(), 2, highlightingListElementMap.size());
      // id element
      {
        final String docId = (String)highlightingListElementMap.get(id_key);
        seenDocIds.add(docId);
        final String preHighlightText;
        final String highlightedText = "bee";
        if ("1".equals(docId)) {
          preHighlightText = "bumble ";
        } else if ("2".equals(docId)) {
          preHighlightText = "honey ";
        } else if ("3".equals(docId)) {
          preHighlightText = "solitary ";
        } else  {
          preHighlightText = null;
          fail("unknown docId "+docId);
        }
        expectedHighlightText = preHighlightText
            + SolrFragmentsBuilder.DEFAULT_PRE_TAGS
            + highlightedText
            + SolrFragmentsBuilder.DEFAULT_POST_TAGS;
      }
      // snippets element
      {
        @SuppressWarnings({"unchecked"})
        SimpleOrderedMap<Object> snippets = (SimpleOrderedMap<Object>)highlightingListElementMap.get(snippets_key);
        if (highlightedField == null) {
          assertEquals(0, snippets.size());
        } else {
          @SuppressWarnings({"unchecked"})
          ArrayList<String> docHighlights = (ArrayList<String>)(snippets).get(highlightedField);
          assertEquals(1, docHighlights.size());
          actualHighlightText = docHighlights.get(0);
          assertEquals(expectedHighlightText, actualHighlightText);
        }
      }
    }
    assertEquals(3, seenDocIds.size());
  }

}
