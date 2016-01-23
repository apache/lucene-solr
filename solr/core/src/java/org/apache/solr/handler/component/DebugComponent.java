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

import static org.apache.solr.common.params.CommonParams.FQ;

import org.apache.solr.common.params.CommonParams;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.SolrPluginUtils;

/**
 * Adds debugging information to a request.
 * 
 *
 * @since solr 1.3
 */
public class DebugComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "debug";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    if( rb.isDebug() ) {

      NamedList stdinfo = SolrPluginUtils.doStandardDebug( rb.req,
          rb.getQueryString(), rb.getQuery(), rb.getResults().docList, rb.isDebugQuery(), rb.isDebugResults());
      
      NamedList info = rb.getDebugInfo();
      if( info == null ) {
        rb.setDebugInfo( stdinfo );
        info = stdinfo;
      }
      else {
        info.addAll( stdinfo );
      }
      
      if (rb.isDebugQuery() && rb.getQparser() != null) {
        rb.getQparser().addDebugInfo(rb.getDebugInfo());
      }

      if (null != rb.getDebugInfo() ) {
        if (rb.isDebugQuery() && null != rb.getFilters() ) {
          info.add("filter_queries",rb.req.getParams().getParams(FQ));
          List<String> fqs = new ArrayList<String>(rb.getFilters().size());
          for (Query fq : rb.getFilters()) {
            fqs.add(QueryParsing.toString(fq, rb.req.getSchema()));
          }
          info.add("parsed_filter_queries",fqs);
        }
        
        // Add this directly here?
        rb.rsp.add("debug", rb.getDebugInfo() );
      }
    }
  }


  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.isDebug()) return;

    // Turn on debug to get explain only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_DEBUG;
      if (rb.isDebugAll()) {
        sreq.params.set(CommonParams.DEBUG_QUERY, "true");
      } else if (rb.isDebugQuery()){
        sreq.params.set(CommonParams.DEBUG, CommonParams.QUERY);
      } else if (rb.isDebugTimings()){
        sreq.params.set(CommonParams.DEBUG, CommonParams.TIMING);
      } else if (rb.isDebugResults()){
        sreq.params.set(CommonParams.DEBUG, CommonParams.RESULTS);
      }
    } else {
      sreq.params.set(CommonParams.DEBUG_QUERY, "false");
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
  }

  private Set<String> excludeSet = new HashSet<String>(Arrays.asList("explain"));

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.isDebug() && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      NamedList<Object> info = null;
      NamedList explain = new SimpleOrderedMap();

      Map.Entry<String, Object>[]  arr =  new NamedList.NamedListEntry[rb.resultIds.size()];

      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_DEBUG) == 0) continue;
        for (ShardResponse srsp : sreq.responses) {
          NamedList sdebug = (NamedList)srsp.getSolrResponse().getResponse().get("debug");
          info = (NamedList)merge(sdebug, info, excludeSet);

          if (rb.isDebugResults()) {
            NamedList sexplain = (NamedList)sdebug.get("explain");
            for (int i = 0; i < sexplain.size(); i++) {
              String id = sexplain.getName(i);
              // TODO: lookup won't work for non-string ids... String vs Float
              ShardDoc sdoc = rb.resultIds.get(id);
              int idx = sdoc.positionInResponse;
              arr[idx] = new NamedList.NamedListEntry<Object>(id, sexplain.getVal(i));
            }
          }
        }
      }

      if (rb.isDebugResults()) {
        explain = SolrPluginUtils.removeNulls(new SimpleOrderedMap<Object>(arr));
      }

      if (info == null) {
        info = new SimpleOrderedMap<Object>();
      }
      if (rb.isDebugResults()) {
        int idx = info.indexOf("explain",0);
        if (idx>=0) {
          info.setVal(idx, explain);
        } else {
          info.add("explain", explain);
        }
      }

      rb.setDebugInfo(info);
      rb.rsp.add("debug", rb.getDebugInfo() );      
    }
  }


  Object merge(Object source, Object dest, Set<String> exclude) {
    if (source == null) return dest;
    if (dest == null) {
      if (source instanceof NamedList) {
        dest = source instanceof SimpleOrderedMap ? new SimpleOrderedMap() : new NamedList();
      } else {
        return source;
      }
    } else {

      if (dest instanceof Collection) {
        if (source instanceof Collection) {
          ((Collection)dest).addAll((Collection)source);
        } else {
          ((Collection)dest).add(source);
        }
        return dest;
      } else if (source instanceof Number) {
        if (dest instanceof Number) {
          if (source instanceof Double || dest instanceof Double) {
            return ((Number)source).doubleValue() + ((Number)dest).doubleValue();
          }
          return ((Number)source).longValue() + ((Number)dest).longValue();
        }
        // fall through
      } else if (source instanceof String) {
        if (source.equals(dest)) {
          return dest;
        }
        // fall through
      }
    }


    if (source instanceof NamedList && dest instanceof NamedList) {
      NamedList<Object> tmp = new NamedList<Object>();
      @SuppressWarnings("unchecked")
      NamedList<Object> sl = (NamedList<Object>)source;
      @SuppressWarnings("unchecked")
      NamedList<Object> dl = (NamedList<Object>)dest;
      for (int i=0; i<sl.size(); i++) {
        String skey = sl.getName(i);
        if (exclude != null && exclude.contains(skey)) continue;
        Object sval = sl.getVal(i);
        int didx = -1;

        // optimize case where elements are in same position
        if (i < dl.size()) {
          String dkey = dl.getName(i);
          if (skey == dkey || (skey!=null && skey.equals(dkey))) {
            didx = i;
          }
        }

        if (didx == -1) {
          didx = dl.indexOf(skey, 0);
        }

        if (didx == -1) {
          tmp.add(skey, merge(sval, null, null));
        } else {
          dl.setVal(didx, merge(sval, dl.getVal(didx), null));
        }
      }
      dl.addAll(tmp);
      return dl;
    }

    // merge unlike elements in a list
    List<Object> t = new ArrayList<Object>();
    t.add(dest);
    t.add(source);
    return t;
  }


  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Debug Information";
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
