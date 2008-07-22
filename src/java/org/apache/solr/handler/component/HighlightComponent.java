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

package org.apache.solr.handler.component;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

/**
 * TODO!
 *
 * @version $Id$
 * @since solr 1.3
 */
public class HighlightComponent extends SearchComponent 
{
  public static final String COMPONENT_NAME = "highlight";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    SolrHighlighter highlighter = rb.req.getCore().getHighlighter();
    rb.doHighlights = highlighter.isHighlightingEnabled(rb.req.getParams());
  }
  
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrQueryRequest req = rb.req;
    if (rb.doHighlights) {
      SolrHighlighter highlighter = req.getCore().getHighlighter();
      SolrParams params = req.getParams();

      String[] defaultHighlightFields;  //TODO: get from builder by default?

      if (rb.getQparser() != null) {
        defaultHighlightFields = rb.getQparser().getDefaultHighlightFields();
      } else {
        defaultHighlightFields = params.getParams(CommonParams.DF);
      }
      
      if(rb.getHighlightQuery()==null) {
        if (rb.getQparser() != null) {
          try {
            rb.setHighlightQuery( rb.getQparser().getHighlightQuery() );
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        } else {
          rb.setHighlightQuery( rb.getQuery() );
        }
      }
      
      NamedList sumData = highlighter.doHighlighting(
              rb.getResults().docList,
              rb.getHighlightQuery().rewrite(req.getSearcher().getReader()),
              req, defaultHighlightFields );
      
      if(sumData != null) {
        // TODO ???? add this directly to the response?
        rb.rsp.add("highlighting", sumData);
      }
    }
  }

  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doHighlights) return;

    // Turn on highlighting only only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
        sreq.purpose |= ShardRequest.PURPOSE_GET_HIGHLIGHTS;
        // should already be true...
        sreq.params.set(HighlightParams.HIGHLIGHT, "true");      
    } else {
      sreq.params.set(HighlightParams.HIGHLIGHT, "false");      
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.doHighlights && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      NamedList hlResult = new SimpleOrderedMap();

      Object[] arr = new Object[rb.resultIds.size() * 2];

      // TODO: make a generic routine to do automatic merging of id keyed data
      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_HIGHLIGHTS) == 0) continue;
        for (ShardResponse srsp : sreq.responses) {
          NamedList hl = (NamedList)srsp.getSolrResponse().getResponse().get("highlighting");
          for (int i=0; i<hl.size(); i++) {
            String id = hl.getName(i);
            ShardDoc sdoc = rb.resultIds.get(id);
            int idx = sdoc.positionInResponse;
            arr[idx<<1] = id;
            arr[(idx<<1)+1] = hl.getVal(i);
          }
        }
      }

      // remove nulls in case not all docs were able to be retrieved
      rb.rsp.add("highlighting", removeNulls(new SimpleOrderedMap(Arrays.asList(arr))));      
    }
  }


  static NamedList removeNulls(NamedList nl) {
    for (int i=0; i<nl.size(); i++) {
      if (nl.getName(i)==null) {
        NamedList newList = nl instanceof SimpleOrderedMap ? new SimpleOrderedMap() : new NamedList();
        for (int j=0; j<nl.size(); j++) {
          String n = nl.getName(j);
          if (n != null) {
            newList.add(n, nl.getVal(j));
          }
        }
        return newList;
      }
    }
    return nl;
  }

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////
  
  @Override
  public String getDescription() {
    return "Highlighting";
  }
  
  @Override
  public String getVersion() {
    return "$Revision$";
  }
  
  @Override
  public String getSourceId() {
    return "$Id$";
  }
  
  @Override
  public String getSource() {
    return "$URL$";
  }
  
  @Override
  public URL[] getDocs() {
    return null;
  }
}
