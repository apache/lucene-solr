package org.apache.solr.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

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

public class StopableIndexingThread extends AbstractFullDistribZkTestBase.StopableThread {
  private static String t1 = "a_t";
  private static String i1 = "a_si";
  private volatile boolean stop = false;
  protected final String id;
  protected final List<String> deletes = new ArrayList<>();
  protected Set<String> addFails = new HashSet<>();
  protected Set<String> deleteFails = new HashSet<>();
  protected boolean doDeletes;
  private int numCycles;
  private SolrServer controlClient;
  private SolrServer cloudClient;
  private int numDeletes;
  private int numAdds;

  public StopableIndexingThread(SolrServer controlClient, SolrServer cloudClient, String id, boolean doDeletes) {
    this(controlClient, cloudClient, id, doDeletes, -1);
  }
  
  public StopableIndexingThread(SolrServer controlClient, SolrServer cloudClient, String id, boolean doDeletes, int numCycles) {
    super("StopableIndexingThread");
    this.controlClient = controlClient;
    this.cloudClient = cloudClient;
    this.id = id;
    this.doDeletes = doDeletes;
    this.numCycles = numCycles;
    setDaemon(true);
  }
  
  @Override
  public void run() {
    int i = 0;
    int numDone = 0;
    numDeletes = 0;
    numAdds = 0;
    
    while (true && !stop) {
      if (numCycles != -1) {
        if (numDone > numCycles) {
          break;
        }
      }
      ++numDone;
      String id = this.id + "-" + i;
      ++i;
      boolean addFailed = false;
      
      if (doDeletes && AbstractFullDistribZkTestBase.random().nextBoolean() && deletes.size() > 0) {
        String deleteId = deletes.remove(0);
        try {
          numDeletes++;
          if (controlClient != null) {
            UpdateRequest req = new UpdateRequest();
            req.deleteById(deleteId);
            req.setParam("CONTROL", "TRUE");
            req.process(controlClient);
          }
          
          cloudClient.deleteById(deleteId);
        } catch (Exception e) {
          System.err.println("REQUEST FAILED for id=" + deleteId);
          e.printStackTrace();
          if (e instanceof SolrServerException) {
            System.err.println("ROOT CAUSE for id=" + deleteId);
            ((SolrServerException) e).getRootCause().printStackTrace();
          }
          deleteFails.add(deleteId);
        }
      }
      
      try {
        numAdds++;
        indexr("id", id, i1, 50, t1,
            "to come to the aid of their country.");
      } catch (Exception e) {
        addFailed = true;
        System.err.println("REQUEST FAILED for id=" + id);
        e.printStackTrace();
        if (e instanceof SolrServerException) {
          System.err.println("ROOT CAUSE for id=" + id);
          ((SolrServerException) e).getRootCause().printStackTrace();
        }
        addFails.add(id);
      }
      
      if (!addFailed && doDeletes && AbstractFullDistribZkTestBase.random().nextBoolean()) {
        deletes.add(id);
      }
      
      try {
        Thread.currentThread().sleep(AbstractFullDistribZkTestBase.random().nextInt(100));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    System.err.println("added docs:" + numAdds + " with " + (addFails.size() + deleteFails.size()) + " fails"
        + " deletes:" + numDeletes);
  }
  
  @Override
  public void safeStop() {
    stop = true;
  }
  
  public Set<String> getAddFails() {
    return addFails;
  }
  
  public Set<String> getDeleteFails() {
    return deleteFails;
  }
  
  public int getFailCount() {
    return addFails.size() + deleteFails.size();
  }
  
  protected void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException,
      SolrServerException {
    
    if (controlClient != null) {
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      req.setParam("CONTROL", "TRUE");
      req.process(controlClient);
    }

    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.process(cloudClient);
  }
  
  public int getNumDeletes() {
    return numDeletes;
  }

  public int getNumAdds() {
    return numAdds;
  }
  
}