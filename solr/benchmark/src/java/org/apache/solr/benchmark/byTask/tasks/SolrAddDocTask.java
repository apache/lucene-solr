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
package org.apache.solr.benchmark.byTask.tasks;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;

public class SolrAddDocTask extends PerfTask {
  
  private volatile Map<String,String> fieldMappings = new HashMap<String,String>();
  
  private final Random r = new Random();
  
  public SolrAddDocTask(PerfRunData runData) {
    super(runData);
  }
  
  private int docSize = 0;
  private int batchSize = 1;
  
  // volatile data passed between setup(), doLogic(), tearDown().
  private Document doc = null;

  private int rndPause = 0;

  private int rndCommit;
  
  @Override
  public void setup() throws Exception {
    try {
    super.setup();
    DocMaker docMaker = getRunData().getDocMaker();
    if (docSize > 0) {
      doc = docMaker.makeDocument(docSize);
    } else {
      doc = docMaker.makeDocument();
    }
    
    // get field mappings - TODO: we don't want to every instance...
    
    String solrFieldMappings = getRunData().getConfig().get(
        "solr.field.mappings", null);
    if (solrFieldMappings != null) {
      String[] mappings = solrFieldMappings.split(",");
      Map<String,String> fieldMappings = new HashMap<String,String>(
          mappings.length);
      for (String mapping : mappings) {
        int index = mapping.indexOf(">");
        if (index == -1) {
          System.err.println("Invalid Solr field mapping:" + mapping);
          continue;
        }
        String from = mapping.substring(0, index);
        String to = mapping.substring(index + 1, mapping.length());
        // System.err.println("From:" + from + " to:" + to);
        fieldMappings.put(from, to);
      }
      this.fieldMappings = fieldMappings;
    }
    } catch(Exception e) {
      e.printStackTrace(new PrintStream(System.out));
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    doc = null;
    fieldMappings.clear();
    super.tearDown();
  }
  
  @Override
  protected String getLogMessage(int recsCount) {
    return "added " + recsCount + " docs";
  }
  
  @Override
  public int doLogic() throws Exception {
    try {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.getParams().set("wt", "xml"); // xml works across more versions
      DocMaker docMaker = getRunData().getDocMaker();
      for (int i = 0; i < batchSize; i++) {
        if (docSize > 0) {
          doc = docMaker.makeDocument(docSize);
        } else {
          doc = docMaker.makeDocument();
        }
        addDoc(ureq, doc);
      }
      SolrClient solrServer = (SolrClient) getRunData().getPerfObject("solr.client");;
      UpdateResponse resp = ureq.process(solrServer);

      int rnd = r.nextInt(100);
      if (rndCommit > 0 &&  rnd < rndCommit) {
        System.out.println("rndCommit:" + rndCommit + " rnd:" + rnd + " so commit");
        AbstractUpdateRequest req = new UpdateRequest()
        .setAction(UpdateRequest.ACTION.COMMIT, false, false);
        req.setParam(UpdateParams.OPEN_SEARCHER, "false");
        req.process(solrServer, null);
      }
      
      if (rndPause > 0) {
        Thread.sleep(r.nextInt(rndPause));
      }
    } catch (Throwable e) {
      e.printStackTrace(new PrintStream(System.out));
      throw new RuntimeException(e);
    }
    return batchSize;
  }

  private void addDoc(UpdateRequest ureq, Document doc) {
    SolrInputDocument solrDoc = new SolrInputDocument();
    List<IndexableField> fields = doc.getFields();
    for (IndexableField field : fields) {
      // System.err.println("field:" + field.name());
      String name = field.name();
      String mappedName = fieldMappings.get(name);
      if (mappedName == null) {
        mappedName = name;
      }
      // System.err.println("mapped field:" + mappedName);
      solrDoc.addField(mappedName, field.stringValue());
    }

    solrDoc.removeField("id");
    solrDoc.addField("id", new UUID(r.nextLong(), r.nextLong()).toString());

    ureq.add(solrDoc);
  }
  
  /**
   * Set the params (docSize only)
   * 
   * @param params
   *          docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    // can't call super because super doesn't understand our
    // params syntax
    this.params = params;
    String [] splits = params.split(",");
    for (int i = 0; i < splits.length; i++) {
      if (splits[i].startsWith("docSize[") == true){
        docSize = (int)Float.parseFloat(splits[i].substring("docSize[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("batchSize[") == true){
        batchSize = (int)Float.parseFloat(splits[i].substring("batchSize[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("rndPause[") == true){
        rndPause  = (int)Float.parseFloat(splits[i].substring("rndPause[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("rndCommit[") == true){
        rndCommit  = (int)Float.parseFloat(splits[i].substring("rndCommit[".length(),splits[i].length() - 1));
      }
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
