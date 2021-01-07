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

package org.apache.solr.client.ref_guide_examples;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

import org.apache.solr.cloud.SolrCloudTestCase;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;

import org.junit.After;
import org.junit.BeforeClass;

/**
 * Example SolrJ usage for indexing nested documents
 *
 * Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference Guide.
 */
public class IndexingNestedDocuments extends SolrCloudTestCase {
  public static final String ANON_KIDS_CONFIG = "anon_kids_configset";
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
      // when indexing 'anonymous' kids, we need a schema that doesn't use _nest_path_ so
      // that we can use [child] transformer with a parentFilter...
      .addConfig(ANON_KIDS_CONFIG, new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
      .configure();
  }

  @After
  public void cleanCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  /**
   * Syntactic sugar so code snippet doesn't refer to test-framework specific method name 
   */
  public static SolrClient getSolrClient() {
    return cluster.getSolrClient();
  }

  /**
   * Demo of using anonymous children when indexing hierarchical documents.
   * This test code is used as an 'include' from the ref-guide
   */
  public void testIndexingAnonKids() throws Exception {
    final String collection = "test_anon";
    CollectionAdminRequest.createCollection(collection, ANON_KIDS_CONFIG, 1, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().setDefaultCollection(collection);
    
    //
    // DO NOT MODIFY THESE EXAMPLE DOCS WITH OUT MAKING THE SAME CHANGES TO THE JSON AND XML
    // EQUIVILENT EXAMPLES IN 'indexing-nested-documents.adoc'
    //

    // tag::anon-kids[]
    final SolrClient client = getSolrClient();

    final SolrInputDocument p1 = new SolrInputDocument();
    p1.setField("id", "P11!prod");
    p1.setField("type_s", "PRODUCT");
    p1.setField("name_s", "Swingline Stapler");
    p1.setField("description_t", "The Cadillac of office staplers ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P11!S21");
      s1.setField("type_s", "SKU");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 42);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P11!D41");
        m1.setField("type_s", "MANUAL");
        m1.setField("name_s", "Red Swingline Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.addChildDocument(m1);
      }

      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P11!S31");
      s2.setField("type_s", "SKU");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 3);

      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P11!D51");
      m1.setField("type_s", "MANUAL");
      m1.setField("name_s", "Quick Reference Guide");
      m1.setField("pages_i", 1);
      m1.setField("content_t", "How to use your stapler ...");

      final SolrInputDocument m2 = new SolrInputDocument();
      m2.setField("id", "P11!D61");
      m2.setField("type_s", "MANUAL");
      m2.setField("name_s", "Warranty Details");
      m2.setField("pages_i", 42);
      m2.setField("content_t", "... lifetime guarantee ...");

      p1.addChildDocuments(Arrays.asList(s1, s2, m1, m2));
    }
    
    client.add(p1);
    // end::anon-kids[]
    
    client.commit();

    final SolrDocumentList docs = getSolrClient().query
      (new SolrQuery("description_t:Cadillac").set("fl", "*,[child parentFilter='type_s:PRODUCT']")).getResults();
    
    assertEquals(1, docs.getNumFound());
    assertEquals("P11!prod", docs.get(0).getFieldValue("id"));

    // [child] returns a flat list of all (anon) descendents
    assertEquals(5, docs.get(0).getChildDocumentCount());
    assertEquals(5, docs.get(0).getChildDocuments().size());

    // flat list is depth first...
    final SolrDocument red_stapler_brochure = docs.get(0).getChildDocuments().get(0);
    assertEquals("P11!D41", red_stapler_brochure.getFieldValue("id"));
    
    final SolrDocument red_stapler = docs.get(0).getChildDocuments().get(1);
    assertEquals("P11!S21", red_stapler.getFieldValue("id"));

  }
  
  /**
   * Demo of using <code>NestPath</code> related psuedo-fields when indexing hierarchical documents.
   * This test code is used as an 'include' from the ref-guide
   */
  public void testIndexingUsingNestPath() throws Exception {
    final String collection = "test_anon";
    CollectionAdminRequest.createCollection(collection, 1, 1).process(cluster.getSolrClient());
    cluster.getSolrClient().setDefaultCollection(collection);
    
    //
    // DO NOT MODIFY THESE EXAMPLE DOCS WITH OUT MAKING THE SAME CHANGES TO THE JSON AND XML
    // EQUIVILENT EXAMPLES IN 'indexing-nested-documents.adoc'
    //

    // tag::nest-path[]
    final SolrClient client = getSolrClient();

    final SolrInputDocument p1 = new SolrInputDocument();
    p1.setField("id", "P11!prod");
    p1.setField("name_s", "Swingline Stapler");
    p1.setField("description_t", "The Cadillac of office staplers ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P11!S21");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 42);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P11!D41");
        m1.setField("name_s", "Red Swingline Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.setField("manuals", m1);
      }

      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P11!S31");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 3);
      
      p1.setField("skus", Arrays.asList(s1, s2));
    }
    {
      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P11!D51");
      m1.setField("name_s", "Quick Reference Guide");
      m1.setField("pages_i", 1);
      m1.setField("content_t", "How to use your stapler ...");

      final SolrInputDocument m2 = new SolrInputDocument();
      m2.setField("id", "P11!D61");
      m2.setField("name_s", "Warranty Details");
      m2.setField("pages_i", 42);
      m2.setField("content_t", "... lifetime guarantee ...");

      p1.setField("manuals", Arrays.asList(m1, m2));
    }

    final SolrInputDocument p2 = new SolrInputDocument();
    p2.setField("id", "P22!prod");
    p2.setField("name_s", "Mont Blanc Fountain Pen");
    p2.setField("description_t", "A Premium Writing Instrument ...");
    {
      final SolrInputDocument s1 = new SolrInputDocument();
      s1.setField("id", "P22!S22");
      s1.setField("color_s", "RED");
      s1.setField("price_i", 89);
      { 
        final SolrInputDocument m1 = new SolrInputDocument();
        m1.setField("id", "P22!D42");
        m1.setField("name_s", "Red Mont Blanc Brochure");
        m1.setField("pages_i", 1);
        m1.setField("content_t", "...");

        s1.setField("manuals", m1);
      }
      
      final SolrInputDocument s2 = new SolrInputDocument();
      s2.setField("id", "P22!S32");
      s2.setField("color_s", "BLACK");
      s2.setField("price_i", 67);
      
      p2.setField("skus", Arrays.asList(s1, s2));
    }
    {
      final SolrInputDocument m1 = new SolrInputDocument();
      m1.setField("id", "P22!D52");
      m1.setField("name_s", "How To Use A Pen");
      m1.setField("pages_i", 42);
      m1.setField("content_t", "Start by removing the cap ...");

      p2.setField("manuals", m1);
    }
    
    client.add(Arrays.asList(p1, p2));
    // end::nest-path[]
    
    client.commit();


    // Now a quick sanity check that the nest path is working properly...

    final SolrDocumentList docs = getSolrClient().query
      (new SolrQuery("description_t:Writing").set("fl", "*,[child]")).getResults();
    
    assertEquals(1, docs.getNumFound());
    assertEquals("P22!prod", docs.get(0).getFieldValue("id"));
    
    assertEquals(1, docs.get(0).getFieldValues("manuals").size());
    assertEquals(SolrDocument.class, docs.get(0).getFieldValues("manuals").iterator().next().getClass());

    assertEquals(2, docs.get(0).getFieldValues("skus").size());
    final List<Object> skus = new ArrayList<>(docs.get(0).getFieldValues("skus"));
    
    assertEquals(SolrDocument.class, skus.get(0).getClass());
    assertEquals(SolrDocument.class, skus.get(1).getClass());
    
    final SolrDocument red_pen = (SolrDocument) skus.get(0);
    assertEquals("P22!S22", red_pen.getFieldValue("id"));

    assertEquals(1, red_pen.getFieldValues("manuals").size());
    assertEquals(SolrDocument.class, red_pen.getFieldValues("manuals").iterator().next().getClass());

    final SolrDocument red_pen_brochure = (SolrDocument) red_pen.getFieldValues("manuals").iterator().next();
    assertEquals("P22!D42", red_pen_brochure.getFieldValue("id"));
  }
}
