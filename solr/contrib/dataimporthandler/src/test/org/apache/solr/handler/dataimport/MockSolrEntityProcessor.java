package org.apache.solr.handler.dataimport;

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

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class MockSolrEntityProcessor extends SolrEntityProcessor {
  
  private final String[][][] docsData;
  private final int rows;
  private int queryCount = 0;
  
  public MockSolrEntityProcessor(String[][][] docsData) {
    this(docsData, ROWS_DEFAULT);
  }
  
  public MockSolrEntityProcessor(String[][][] docsData, int rows) {
    this.docsData = docsData;
    this.rows = rows;
  }
  
  @Override
  protected SolrDocumentList doQuery(int start) {
    queryCount++;
    return getDocs(start, rows);
  }
  
  private SolrDocumentList getDocs(int start, int rows) {
    SolrDocumentList docs = new SolrDocumentList();
    docs.setNumFound(docsData.length);
    docs.setStart(start);
    
    int endIndex = start + rows;
    int end = docsData.length < endIndex ? docsData.length : endIndex;
    for (int i = start; i < end; i++) {
      SolrDocument doc = new SolrDocument();
      for (String[] fields : docsData[i]) {
        doc.addField(fields[0], fields[1]);
      }
      docs.add(doc);
    }
    return docs;
  }

  public int getQueryCount() {
    return queryCount;
  }
}
