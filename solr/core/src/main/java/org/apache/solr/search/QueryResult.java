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
package org.apache.solr.search;

/**
 * The result of a search.
 */
public class QueryResult {
  
  private boolean partialResults;
  private Boolean segmentTerminatedEarly;
  private DocListAndSet docListAndSet;
  private CursorMark nextCursorMark;
  
  public Object groupedResults; // TODO: currently for testing
  
  public DocList getDocList() {
    return docListAndSet.docList;
  }
  
  public void setDocList(DocList list) {
    if (docListAndSet == null) {
      docListAndSet = new DocListAndSet();
    }
    docListAndSet.docList = list;
  }
  
  public DocSet getDocSet() {
    return docListAndSet.docSet;
  }
  
  public void setDocSet(DocSet set) {
    if (docListAndSet == null) {
      docListAndSet = new DocListAndSet();
    }
    docListAndSet.docSet = set;
  }
  
  public boolean isPartialResults() {
    return partialResults;
  }
  
  public void setPartialResults(boolean partialResults) {
    this.partialResults = partialResults;
  }
  
  public Boolean getSegmentTerminatedEarly() {
    return segmentTerminatedEarly;
  }

  public void setSegmentTerminatedEarly(Boolean segmentTerminatedEarly) {
    this.segmentTerminatedEarly = segmentTerminatedEarly;
  }

  public void setDocListAndSet(DocListAndSet listSet) {
    docListAndSet = listSet;
  }
  
  public DocListAndSet getDocListAndSet() {
    return docListAndSet;
  }
  
  public void setNextCursorMark(CursorMark next) {
    this.nextCursorMark = next;
  }
  
  public CursorMark getNextCursorMark() {
    return nextCursorMark;
  }
  
}
