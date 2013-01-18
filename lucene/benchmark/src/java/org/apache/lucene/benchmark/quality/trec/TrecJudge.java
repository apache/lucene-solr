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
package org.apache.lucene.benchmark.quality.trec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.lucene.benchmark.quality.Judge;
import org.apache.lucene.benchmark.quality.QualityQuery;

/**
 * Judge if given document is relevant to given quality query, based on Trec format for judgements.
 */
public class TrecJudge implements Judge {

  HashMap<String,QRelJudgement> judgements;
  
  /**
   * Constructor from a reader.
   * <p>
   * Expected input format:
   * <pre>
   *     qnum  0   doc-name     is-relevant
   * </pre> 
   * Two sample lines:
   * <pre> 
   *     19    0   doc303       1
   *     19    0   doc7295      0
   * </pre> 
   * @param reader where judgments are read from.
   * @throws IOException If there is a low-level I/O error.
   */
  public TrecJudge (BufferedReader reader) throws IOException {
    judgements = new HashMap<String,QRelJudgement>();
    QRelJudgement curr = null;
    String zero = "0";
    String line;
    
    try {
      while (null!=(line=reader.readLine())) {
        line = line.trim();
        if (line.length()==0 || '#'==line.charAt(0)) {
          continue;
        }
        StringTokenizer st = new StringTokenizer(line);
        String queryID = st.nextToken();
        st.nextToken();
        String docName = st.nextToken();
        boolean relevant = !zero.equals(st.nextToken());
        assert !st.hasMoreTokens() : "wrong format: "+line+"  next: "+st.nextToken();
        if (relevant) { // only keep relevant docs
          if (curr==null || !curr.queryID.equals(queryID)) {
            curr = judgements.get(queryID);
            if (curr==null) {
              curr = new QRelJudgement(queryID);
              judgements.put(queryID,curr);
            }
          }
          curr.addRelevandDoc(docName);
        }
      }
    } finally {
      reader.close();
    }
  }
  
  // inherit javadocs
  @Override
  public boolean isRelevant(String docName, QualityQuery query) {
    QRelJudgement qrj = judgements.get(query.getQueryID());
    return qrj!=null && qrj.isRelevant(docName);
  }

  /** single Judgement of a trec quality query */
  private static class QRelJudgement {
    private String queryID;
    private HashMap<String,String> relevantDocs;
    
    QRelJudgement(String queryID) {
      this.queryID = queryID;
      relevantDocs = new HashMap<String,String>();
    }
    
    public void addRelevandDoc(String docName) {
      relevantDocs.put(docName,docName);
    }

    boolean isRelevant(String docName) {
      return relevantDocs.containsKey(docName);
    }

    public int maxRecall() {
      return relevantDocs.size();
    }
  }

  // inherit javadocs
  @Override
  public boolean validateData(QualityQuery[] qq, PrintWriter logger) {
    HashMap<String,QRelJudgement> missingQueries = new HashMap<String, QRelJudgement>(judgements);
    ArrayList<String> missingJudgements = new ArrayList<String>();
    for (int i=0; i<qq.length; i++) {
      String id = qq[i].getQueryID();
      if (missingQueries.containsKey(id)) {
        missingQueries.remove(id);
      } else {
        missingJudgements.add(id);
      }
    }
    boolean isValid = true;
    if (missingJudgements.size()>0) {
      isValid = false;
      if (logger!=null) {
        logger.println("WARNING: "+missingJudgements.size()+" queries have no judgments! - ");
        for (int i=0; i<missingJudgements.size(); i++) {
          logger.println("   "+ missingJudgements.get(i));
        }
      }
    }
    if (missingQueries.size()>0) {
      isValid = false;
      if (logger!=null) {
        logger.println("WARNING: "+missingQueries.size()+" judgments match no query! - ");
        for (final String id : missingQueries.keySet()) {
          logger.println("   "+id);
        }
      }
    }
    return isValid;
  }

  // inherit javadocs
  @Override
  public int maxRecall(QualityQuery query) {
    QRelJudgement qrj = judgements.get(query.getQueryID());
    if (qrj!=null) {
      return qrj.maxRecall();
    }
    return 0;
  }
}
