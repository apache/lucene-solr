package org.apache.solr.core;

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

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;

public class TestMergePolicyConfig extends SolrTestCaseJ4 {

  @After
  public void after() throws Exception {
    deleteCore();
  }

  public void testDefaultMergePolicyConfig() throws Exception {
    initCore("solrconfig-mergepolicy-defaults.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());

    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());
    assertEquals(0.0D, tieredMP.getNoCFSRatio(), 0.0D);

  }

  public void testLegacyMergePolicyConfig() throws Exception {
    initCore("solrconfig-mergepolicy-legacy.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());
    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());

    assertEquals(7, tieredMP.getMaxMergeAtOnce());
    assertEquals(7.0D, tieredMP.getSegmentsPerTier(), 0.0D);
    assertEquals(1.0D, tieredMP.getNoCFSRatio(), 0.0D);
  }
  
  public void testTieredMergePolicyConfig() throws Exception {
    initCore("solrconfig-mergepolicy.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());
    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());

    // set by legacy <mergeFactor> setting
    assertEquals(7, tieredMP.getMaxMergeAtOnce());
    
    // mp-specific setters
    assertEquals(19, tieredMP.getMaxMergeAtOnceExplicit());
    assertEquals(0.6D, tieredMP.getNoCFSRatio(), 0.001);
    // make sure we overrode segmentsPerTier 
    // (split from maxMergeAtOnce out of mergeFactor)
    assertEquals(9D, tieredMP.getSegmentsPerTier(), 0.001);
    
  }

  /**
   * Given a Type and an object asserts that the object is non-null and an 
   * instance of the specified Type.  The object is then cast to that type and 
   * returned.
   */
  public static <T> T assertAndCast(Class<? extends T> clazz, Object o) {
    assertNotNull(clazz);
    assertNotNull(o);
    assertTrue(clazz.isInstance(o));
    return clazz.cast(o);
  }

}
