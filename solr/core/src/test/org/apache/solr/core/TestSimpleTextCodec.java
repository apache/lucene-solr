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

package org.apache.solr.core;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;

public class TestSimpleTextCodec extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig_SimpleTextCodec.xml", "schema-SimpleTextCodec.xml");
  }

  public void test() throws Exception {
    SolrConfig config = h.getCore().getSolrConfig();
    String codecFactory =  config.get("codecFactory").attr("class");
    assertEquals("Unexpected solrconfig codec factory", "solr.SimpleTextCodecFactory", codecFactory);

    assertEquals("Unexpected core codec", "SimpleText", h.getCore().getCodec().getName());

    RefCounted<IndexWriter> writerRef = h.getCore().getSolrCoreState().getIndexWriter(h.getCore());
    try {
      IndexWriter writer = writerRef.get();
      assertEquals("Unexpected codec in IndexWriter config", 
          "SimpleText", writer.getConfig().getCodec().getName()); 
    } finally {
      writerRef.decref();
    }

    assertU(add(doc("id","1", "text","textual content goes here")));
    assertU(commit());

    h.getCore().withSearcher(searcher -> {
      SegmentInfos infos = SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());
      SegmentInfo info = infos.info(infos.size() - 1).info;
      assertEquals("Unexpected segment codec", "SimpleText", info.getCodec().getName());
      return null;
    });

    assertQ(req("q", "id:1"),
        "*[count(//doc)=1]");
  }
}
