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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class MBeansHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testDiff() throws Exception {
    String xml = h.query(req(
        CommonParams.QT,"/admin/mbeans",
        "stats","true",
        CommonParams.WT,"xml"
     ));
    List<ContentStream> streams = new ArrayList<ContentStream>();
    streams.add(new ContentStreamBase.StringStream(xml));
    
    LocalSolrQueryRequest req = lrf.makeRequest(
        CommonParams.QT,"/admin/mbeans",
        "stats","true",
        CommonParams.WT,"xml",
        "diff","true");
    req.setContentStreams(streams);
    
    xml = h.query(req);
    NamedList<NamedList<NamedList<Object>>> diff = SolrInfoMBeanHandler.fromXML(xml);

    // The stats bean for SolrInfoMBeanHandler
    NamedList stats = (NamedList)diff.get("QUERYHANDLER").get("/admin/mbeans").get("stats");
    
    //System.out.println("stats:"+stats);
    assertEquals("Was: 1, Now: 2, Delta: 1", stats.get("requests"));
  }
}
