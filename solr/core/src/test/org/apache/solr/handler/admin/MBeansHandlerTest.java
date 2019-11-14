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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
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
    List<ContentStream> streams = new ArrayList<>();
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
    NamedList stats = (NamedList)diff.get("ADMIN").get("/admin/mbeans").get("stats");

    //System.out.println("stats:"+stats);
    Pattern p = Pattern.compile("Was: (?<was>[0-9]+), Now: (?<now>[0-9]+), Delta: (?<delta>[0-9]+)");
    String response = stats.get("ADMIN./admin/mbeans.requests").toString();
    Matcher m = p.matcher(response);
    if (!m.matches()) {
      fail("Response did not match pattern: " + response);
    }

    assertEquals(1, Integer.parseInt(m.group("delta")));
    int was = Integer.parseInt(m.group("was"));
    int now = Integer.parseInt(m.group("now"));
    assertEquals(1, now - was);

    xml = h.query(req(
        CommonParams.QT,"/admin/mbeans",
        "stats","true",
        "key","org.apache.solr.handler.admin.CollectionsHandler"
    ));
    NamedList<NamedList<NamedList<Object>>> nl = SolrInfoMBeanHandler.fromXML(xml);
    assertNotNull( nl.get("ADMIN").get("org.apache.solr.handler.admin.CollectionsHandler"));
  }

  @Test
  public void testAddedMBeanDiff() throws Exception {
    String xml = h.query(req(
        CommonParams.QT,"/admin/mbeans",
        "stats","true",
        CommonParams.WT,"xml"
    ));

    // Artificially convert a long value to a null, to trigger the ADD case in SolrInfoMBeanHandler.diffObject()
    xml = xml.replaceFirst("<long\\s+(name\\s*=\\s*\"ADMIN./admin/mbeans.totalTime\"\\s*)>[^<]*</long>", "<null $1/>");

    LocalSolrQueryRequest req = lrf.makeRequest(
        CommonParams.QT,"/admin/mbeans",
        "stats","true",
        CommonParams.WT,"xml",
        "diff","true");
    req.setContentStreams(Collections.singletonList(new ContentStreamBase.StringStream(xml)));
    xml = h.query(req);

    NamedList<NamedList<NamedList<Object>>> nl = SolrInfoMBeanHandler.fromXML(xml);
    assertNotNull(((NamedList)nl.get("ADMIN").get("/admin/mbeans").get("stats")).get("ADD ADMIN./admin/mbeans.totalTime"));
  }

  @Test
  public void testXMLDiffWithExternalEntity() throws Exception {
    String file = getFile("mailing_lists.pdf").toURI().toASCIIString();
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<!DOCTYPE foo [<!ENTITY bar SYSTEM \""+file+"\">]>\n" +
        "<response>\n" +
        "&bar;" +
        "<lst name=\"responseHeader\"><int name=\"status\">0</int><int name=\"QTime\">31</int></lst><lst name=\"solr-mbeans\"></lst>\n" +
        "</response>";

    NamedList<NamedList<NamedList<Object>>> nl = SolrInfoMBeanHandler.fromXML(xml);

    assertTrue("external entity ignored properly", true);
  }

  boolean runSnapshots;

  @Test
  public void testMetricsSnapshot() throws Exception {
    final CountDownLatch counter = new CountDownLatch(500);
    SolrInfoBean bean = new SolrInfoBean() {
      SolrMetricsContext solrMetricsContext;
      @Override
      public String getName() {
        return "foo";
      }

      @Override
      public String getDescription() {
        return "foo";
      }

      @Override
      public Category getCategory() {
        return Category.ADMIN;
      }

      @Override
      public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
        this.solrMetricsContext = parentContext.getChildContext(this);
      }

      @Override
      public SolrMetricsContext getSolrMetricsContext() {
        return solrMetricsContext;
      }
    };
    bean.initializeMetrics(new SolrMetricsContext(h.getCoreContainer().getMetricManager(), "testMetricsSnapshot", "foobar"), "foo");
    runSnapshots = true;
    Thread modifier = new Thread(() -> {
      int i = 0;
      while (runSnapshots) {
        bean.getSolrMetricsContext().registerMetricName("name-" + i++);
        try {
          Thread.sleep(31);
        } catch (InterruptedException e) {
          runSnapshots = false;
          break;
        }
      }
    });
    Thread reader = new Thread(() -> {
      while (runSnapshots) {
        try {
          bean.getSolrMetricsContext().getMetricsSnapshot();
        } catch (Exception e) {
          runSnapshots = false;
          e.printStackTrace();
          fail("Exception getting metrics snapshot: " + e.toString());
        }
        try {
          Thread.sleep(53);
        } catch (InterruptedException e) {
          runSnapshots = false;
          break;
        }
        counter.countDown();
      }
    });
    modifier.start();
    reader.start();
    counter.await(30, TimeUnit.SECONDS);
    runSnapshots = false;
  }
}
