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
package org.apache.solr.handler.admin;

import org.apache.lucene.util.Version;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for SegmentsInfoRequestHandler. Plugin entry, returning data of created segment.
 */
public class SegmentsInfoRequestHandlerTest extends AbstractSolrTestCase {
  private static final int DOC_COUNT = 5;
  
  private static final int DEL_COUNT = 1;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false");
    System.setProperty("solr.tests.useMergePolicy", "false");
    initCore("solrconfig.xml", "schema12.xml");
  }

  @Before
  public void before() throws Exception {
    for (int i = 0; i < DOC_COUNT; i++) {
      assertU(adoc("id","SOLR100" + i, "name","Apache Solr:" + i));
    }
    for (int i = 0; i < DEL_COUNT; i++) {
      assertU(delI("SOLR100" + i));
    }
    assertU(commit());
    for (int i = 0; i < DOC_COUNT; i++) {
      assertU(adoc("id","SOLR200" + i, "name","Apache Solr:" + i));
    }
    assertU(commit());
  }

  @Test
  public void testSegmentInfos() {   
    assertQ("No segments mentioned in result",
        req("qt","/admin/segments"),
          "0<count(//lst[@name='segments']/lst)");
  }

  @Test
  public void testSegmentInfosVersion() {
    assertQ("No segments mentioned in result",
        req("qt","/admin/segments"),
        "2=count(//lst[@name='segments']/lst/str[@name='version'][.='"+Version.LATEST+"'])");
  }
  
  @Test
  public void testSegmentInfosData() {   
    assertQ("No segments mentioned in result",
        req("qt","/admin/segments"),
          //#Document
          (DOC_COUNT*2)+"=sum(//lst[@name='segments']/lst/int[@name='size'])",
          //#Deletes
          DEL_COUNT+"=sum(//lst[@name='segments']/lst/int[@name='delCount'])");
  }
}
