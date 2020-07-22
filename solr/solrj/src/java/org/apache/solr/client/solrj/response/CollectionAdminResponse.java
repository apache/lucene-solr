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
package org.apache.solr.client.solrj.response;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.util.NamedList;

public class CollectionAdminResponse extends SolrResponseBase
{
  @SuppressWarnings("unchecked")
  public NamedList<NamedList<Object>> getCollectionStatus()
  {
    return (NamedList<NamedList<Object>>) getResponse().get( "success" );
  }

  public boolean isSuccess()
  {
    return getResponse().get( "success" ) != null;
  }

  public String getWarning()
  {
    return (String) getResponse().get( "warning" );
  }

  // this messages are typically from individual nodes, since
  // all the failures at the router are propagated as exceptions
  @SuppressWarnings("unchecked")
  public NamedList<String> getErrorMessages()
  {
     return (NamedList<String>) getResponse().get( "failure" );
  }

  @SuppressWarnings("unchecked")
  public Map<String, NamedList<Integer>> getCollectionCoresStatus()
  {
    Map<String, NamedList<Integer>> res = new HashMap<>();
    NamedList<NamedList<Object>> cols = getCollectionStatus();
    if( cols != null ) {
      for (Map.Entry<String, NamedList<Object>> e : cols) {
        NamedList<Object> item = e.getValue();
        String core = (String) item.get("core");
        if (core != null) {
          res.put(core, (NamedList<Integer>)item.get("responseHeader"));
        }
      }
    }

    return res;
  }

  @SuppressWarnings("unchecked")
  public Map<String, String> getAliases()
  {
    NamedList<Object> response = getResponse();
    if (response.get("aliases") != null) {
      return ((Map<String, String>)response.get("aliases"));
    }
    return Collections.emptyMap();
  }

  public Map<String, List<String>> getAliasesAsLists() {
    // TODO we compute on each call... should this be done once & cached?
    return Aliases.convertMapOfCommaDelimitedToMapOfList(getAliases());
  }

  @SuppressWarnings({"unchecked"})
  public Map<String, Map<String, String>> getAliasProperties() {
    NamedList<Object> response = getResponse();
    if (response.get("properties") != null) {
      return ((Map<String, Map<String, String>>)response.get("properties"));
    }
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  public Map<String, NamedList<Integer>> getCollectionNodesStatus()
  {
    Map<String, NamedList<Integer>> res = new HashMap<>();
    NamedList<NamedList<Object>> cols = getCollectionStatus();
    if( cols != null ) {
      for (Map.Entry<String,NamedList<Object>> e : cols) {
        if (e.getKey() != null) {
          res.put(e.getKey(), (NamedList<Integer>) (e.getValue().get("responseHeader")));
        }
      }
    }

    return res;
  }
}