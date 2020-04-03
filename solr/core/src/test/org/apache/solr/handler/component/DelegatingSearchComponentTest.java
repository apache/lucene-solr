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

import java.lang.reflect.Method;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class DelegatingSearchComponentTest extends SolrCloudTestCase {

  static final String FOOBAR_ANSWER_KEY = "foobar.answer";

  public static  class FooQueryComponent extends QueryComponent {
    public static final String COMPONENT_NAME = "foo";
    @Override
    public void finishStage(ResponseBuilder rb) {
      super.finishStage(rb);
      if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
        rb.rsp.add(FOOBAR_ANSWER_KEY, "flower");
      }
    }
  }

  public static  class BarQueryComponent extends QueryComponent {
    public static final String COMPONENT_NAME = "bar";
    @Override
    public void finishStage(ResponseBuilder rb) {
      super.finishStage(rb);
      if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
        rb.rsp.add(FOOBAR_ANSWER_KEY, "bee");
      }
    }
  }

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
    .setMaxShardsPerNode(maxShardsPerNode)
    .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @Test
  public void test() throws Exception {
    // add custom components
    {
        cluster.getSolrClient().request(
            new ConfigRequest(
                "{\n" +
                "  'add-searchcomponent': {\n" +
                "    'name': '"+FooQueryComponent.COMPONENT_NAME+"',\n" +
                "    'class': '"+FooQueryComponent.class.getName()+"',\n" +
                "    'answer': 'foo' \n" +
                "  }\n" +
                "}"),
            COLLECTION);

        cluster.getSolrClient().request(
            new ConfigRequest(
                "{\n" +
                "  'add-searchcomponent': {\n" +
                "    'name': '"+BarQueryComponent.COMPONENT_NAME+"',\n" +
                "    'class': '"+BarQueryComponent.class.getName()+"',\n" +
                "    'answer': 'bar' \n" +
                "  }\n" +
                "}"),
            COLLECTION);

        cluster.getSolrClient().request(
            new ConfigRequest(
                "{\n" +
                "  'add-searchcomponent': {\n" +
                "    'name': '"+QueryComponent.COMPONENT_NAME+"',\n" +
                "    'class': '"+DelegatingSearchComponent.class.getName()+"',\n" +
                "    'mappings': {\n" +
                "      'delegate.plant' : '"+FooQueryComponent.COMPONENT_NAME+"',\n" +
                "      'delegate.animal' : '"+BarQueryComponent.COMPONENT_NAME+"'\n" +
                "    }\n" +
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
      final String[] queryParameters = new String[] { "delegate.plant", "delegate.animal" };
      final String[] responseAnswers = new String[] { "flower", "bee" };

      for (int ii = 0; ii < queryParameters.length; ++ii) {

        final SolrQuery solrQuery =  new SolrQuery("*:*");
        solrQuery.set(queryParameters[ii], true);

        final QueryResponse queryResponse = new QueryRequest(solrQuery)
            .process(cluster.getSolrClient(), COLLECTION);

        assertEquals(responseAnswers[ii], queryResponse.getResponse().get(FOOBAR_ANSWER_KEY));
      }
    }
  }

  public void testDeclaredMethodsOverridden() throws Exception {
    final Class<?> subClass = DelegatingSearchComponent.class;
    implTestDeclaredMethodsOverridden(subClass.getSuperclass(), subClass);
  }

  private void implTestDeclaredMethodsOverridden(Class<?> superClass, Class<?> subClass) throws Exception {
    for (final Method superClassMethod : superClass.getDeclaredMethods()) {

      // the component's name applies to itself and cannot be delegated
      if (superClassMethod.getName().equals("getName") ||
          superClassMethod.getName().equals("setName")) continue;

      try {
        final Method subClassMethod = subClass.getDeclaredMethod(
            superClassMethod.getName(),
            superClassMethod.getParameterTypes());
        assertEquals("getReturnType() difference",
            superClassMethod.getReturnType(),
            subClassMethod.getReturnType());
      } catch (NoSuchMethodException e) {
        fail(subClass + " needs to override '" + superClassMethod + "'");
      }
    }
  }

}
