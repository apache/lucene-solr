/**
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

package org.apache.solr.response;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;

public class PageTool {
  private long start;
  private int results_per_page = 10;
  private long results_found;
  private int page_count;
  private int current_page_number;

  public PageTool(SolrQueryRequest request, SolrQueryResponse response) {
    String rows = request.getParams().get("rows");

    if (rows != null) {
      results_per_page = new Integer(rows);
    }
    //TODO: Handle group by results
    Object docs = response.getValues().get("response");
    if (docs != null) {
      if (docs instanceof DocSlice) {
        DocSlice doc_slice = (DocSlice) docs;
        results_found = doc_slice.matches();
        start = doc_slice.offset();
      } else if(docs instanceof ResultContext) {
        DocList dl = ((ResultContext) docs).docs;
        results_found = dl.matches();
        start = dl.offset();
      } else if(docs instanceof SolrDocumentList) {
        SolrDocumentList doc_list = (SolrDocumentList) docs;
        results_found = doc_list.getNumFound();
        start = doc_list.getStart();
      } else {
	  throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Unknown response type "+docs+". Expected one of DocSlice, ResultContext or SolrDocumentList");
      }
    }

    page_count = (int) Math.ceil(results_found / (double) results_per_page);
    current_page_number = (int) Math.ceil(start / (double) results_per_page) + (page_count > 0 ? 1 : 0);
  }

  public long getStart() {
    return start;
  }

  public int getResults_per_page() {
    return results_per_page;
  }

  public long getResults_found() {
    return results_found;
  }

  public int getPage_count() {
    return page_count;
  }

  public int getCurrent_page_number() {
    return current_page_number;
  }

  @Override
  public String toString() {
    return "Found " + results_found +
           " Page " + current_page_number + " of " + page_count +
           " Starting at " + start + " per page " + results_per_page;
  }
}
