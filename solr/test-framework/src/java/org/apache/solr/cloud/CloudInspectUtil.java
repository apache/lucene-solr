package org.apache.solr.cloud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class CloudInspectUtil {
  static Logger log = LoggerFactory.getLogger(CloudInspectUtil.class);
  
  /**
   * When a and b are known to be different, this method tells if the difference
   * is legal given the adds and deletes that failed from b.
   * 
   * @param a first list of docs
   * @param b second list of docs
   * @param aName label for first list of docs
   * @param bName  label for second list of docs
   * @param bAddFails null or list of the ids of adds that failed for b
   * @param bDeleteFails null or list of the ids of deletes that failed for b
   * @return true if the difference in a and b is legal
   */
  public static boolean checkIfDiffIsLegal(SolrDocumentList a,
      SolrDocumentList b, String aName, String bName, Set<String> bAddFails,
      Set<String> bDeleteFails) {
    boolean legal = true;
    
    Set<Map> setA = new HashSet<>();
    for (SolrDocument sdoc : a) {
      setA.add(new HashMap(sdoc));
    }
    
    Set<Map> setB = new HashSet<>();
    for (SolrDocument sdoc : b) {
      setB.add(new HashMap(sdoc));
    }
    
    Set<Map> onlyInA = new HashSet<>(setA);
    onlyInA.removeAll(setB);
    Set<Map> onlyInB = new HashSet<>(setB);
    onlyInB.removeAll(setA);
    
    if (onlyInA.size() == 0 && onlyInB.size() == 0) {
      throw new IllegalArgumentException("No difference between list a and b");
    }
    
    System.err.println("###### Only in " + aName + ": " + onlyInA);
    System.err.println("###### Only in " + bName + ": " + onlyInB);
    
    for (Map doc : onlyInA) {
      if (bAddFails == null || !bAddFails.contains(doc.get("id"))) {
        legal = false;
        // System.err.println("###### Only in " + aName + ": " + doc.get("id"));
      } else {
        System.err.println("###### Only in " + aName + ": " + doc.get("id")
            + ", but this is expected because we found an add fail for "
            + doc.get("id"));
      }
    }
    
    for (Map doc : onlyInB) {
      if (bDeleteFails == null || !bDeleteFails.contains(doc.get("id"))) {
        legal = false;
        // System.err.println("###### Only in " + bName + ": " + doc.get("id"));
      } else {
        System.err.println("###### Only in " + bName + ": " + doc.get("id")
            + ", but this is expected because we found a delete fail for "
            + doc.get("id"));
      }
    }
    
    return legal;
  }
  
  /**
   * Shows the difference between two lists of documents.
   * 
   * @param a the first list
   * @param b the second list
   * @param aName label for the first list
   * @param bName label for the second list
   * @return the documents only in list a
   */
  public static Set<Map> showDiff(SolrDocumentList a, SolrDocumentList b,
      String aName, String bName) {
    System.err.println("######" + aName + ": " + toStr(a, 10));
    System.err.println("######" + bName + ": " + toStr(b, 10));
    System.err.println("###### sizes=" + a.size() + "," + b.size());
    
    Set<Map> setA = new HashSet<>();
    for (SolrDocument sdoc : a) {
      setA.add(new HashMap(sdoc));
    }
    
    Set<Map> setB = new HashSet<>();
    for (SolrDocument sdoc : b) {
      setB.add(new HashMap(sdoc));
    }
    
    Set<Map> onlyInA = new HashSet<>(setA);
    onlyInA.removeAll(setB);
    Set<Map> onlyInB = new HashSet<>(setB);
    onlyInB.removeAll(setA);
    
    if (onlyInA.size() > 0) {
      System.err.println("###### Only in " + aName + ": " + onlyInA);
    }
    if (onlyInB.size() > 0) {
      System.err.println("###### Only in " + bName + ": " + onlyInB);
    }
    
    onlyInA.addAll(onlyInB);
    return onlyInA;
  }
  
  private static String toStr(SolrDocumentList lst, int maxSz) {
    if (lst.size() <= maxSz) return lst.toString();
    
    StringBuilder sb = new StringBuilder("SolrDocumentList[sz=" + lst.size());
    if (lst.size() != lst.getNumFound()) {
      sb.append(" numFound=" + lst.getNumFound());
    }
    sb.append("]=");
    sb.append(lst.subList(0, maxSz / 2).toString());
    sb.append(" , [...] , ");
    sb.append(lst.subList(lst.size() - maxSz / 2, lst.size()).toString());
    
    return sb.toString();
  }
  

  /**
   * Compares the results of the control and cloud clients.
   * 
   * @return true if the compared results are illegal.
   */
  public static boolean compareResults(SolrServer controlServer, SolrServer cloudServer)
      throws SolrServerException {
    return compareResults(controlServer, cloudServer, null, null);
  }
  
  /**
   * Compares the results of the control and cloud clients.
   * 
   * @return true if the compared results are illegal.
   */
  public static boolean compareResults(SolrServer controlServer, SolrServer cloudServer, Set<String> addFails, Set<String> deleteFails)
      throws SolrServerException {
    
    SolrParams q = SolrTestCaseJ4.params("q","*:*","rows","0", "tests","checkShardConsistency(vsControl)");    // add a tag to aid in debugging via logs

    SolrDocumentList controlDocList = controlServer.query(q).getResults();
    long controlDocs = controlDocList.getNumFound();

    SolrDocumentList cloudDocList = cloudServer.query(q).getResults();
    long cloudClientDocs = cloudDocList.getNumFound();
    
    // re-execute the query getting ids
    q = SolrTestCaseJ4.params("q","*:*","rows","100000", "fl","id", "tests","checkShardConsistency(vsControl)/getIds");    // add a tag to aid in debugging via logs
    controlDocList = controlServer.query(q).getResults();
    if (controlDocs != controlDocList.getNumFound()) {
      log.error("Something changed! control now " + controlDocList.getNumFound());
    };

    cloudDocList = cloudServer.query(q).getResults();
    if (cloudClientDocs != cloudDocList.getNumFound()) {
      log.error("Something changed! cloudClient now " + cloudDocList.getNumFound());
    };

    if (controlDocs != cloudClientDocs && (addFails != null || deleteFails != null)) {
      boolean legal = CloudInspectUtil.checkIfDiffIsLegal(controlDocList, cloudDocList,
          "controlDocList", "cloudDocList", addFails, deleteFails);
      if (legal) {
        return false;
      }
    }
    
    Set<Map> differences = CloudInspectUtil.showDiff(controlDocList, cloudDocList,
        "controlDocList", "cloudDocList");

    // get versions for the mismatched ids
    boolean foundId = false;
    StringBuilder ids = new StringBuilder("id:(");
    for (Map doc : differences) {
      ids.append(" "+doc.get("id"));
      foundId = true;
    }
    ids.append(")");
    
    if (foundId) {
      // get versions for those ids that don't match
      q = SolrTestCaseJ4.params("q", ids.toString(), "rows", "100000", "fl", "id,_version_",
          "sort", "id asc", "tests",
          "checkShardConsistency(vsControl)/getVers"); // add a tag to aid in
                                                       // debugging via logs
      
      SolrDocumentList a = controlServer.query(q).getResults();
      SolrDocumentList b = cloudServer.query(q).getResults();
      
      log.error("controlClient :" + a + "\n\tcloudClient :" + b);
    }
    
    return true;
  }
}
