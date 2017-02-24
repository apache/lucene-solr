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
package org.apache.solr.handler.dataimport;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.List;

public class MockSolrEntityProcessor extends SolrEntityProcessor {

  private final List<SolrTestCaseJ4.Doc> docsData;
//  private final int rows;
  private int queryCount = 0;

  private int rows;
  
  private int start = 0;

  public MockSolrEntityProcessor(List<SolrTestCaseJ4.Doc> docsData, int rows) {
    this.docsData = docsData;
    this.rows = rows;
  }

  //@Override
  //protected SolrDocumentList doQuery(int start) {
  //  queryCount++;
  //  return getDocs(start, rows);
 // }
  
  @Override
  protected void buildIterator() {
    if (rowIterator==null || (!rowIterator.hasNext() && ((SolrDocumentListIterator)rowIterator).hasMoreRows())){
      queryCount++;
      SolrDocumentList docs = getDocs(start, rows);
      rowIterator = new SolrDocumentListIterator(docs);
      start += docs.size();
    }
  }

  private SolrDocumentList getDocs(int start, int rows) {
    SolrDocumentList docs = new SolrDocumentList();
    docs.setNumFound(docsData.size());
    docs.setStart(start);

    int endIndex = start + rows;
    int end = docsData.size() < endIndex ? docsData.size() : endIndex;
    for (int i = start; i < end; i++) {
      SolrDocument doc = new SolrDocument();
      SolrTestCaseJ4.Doc testDoc = docsData.get(i);
      doc.addField("id", testDoc.id);
      doc.addField("description", testDoc.getValues("description"));
      docs.add(doc);
    }
    return docs;
  }

  public int getQueryCount() {
    return queryCount;
  }
}
