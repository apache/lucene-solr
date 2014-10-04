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

package org.apache.solr.handler;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.InitParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.PluginInfoInitialized;

public class DumpRequestHandler extends RequestHandlerBase
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    // Show params
    rsp.add( "params", req.getParams().toNamedList() );
    String[] returnParams = req.getParams().getParams("param");
    if(returnParams !=null) {
      NamedList params = (NamedList) rsp.getValues().get("params");
      for (String returnParam : returnParams) {
        String[] vals = req.getParams().getParams(returnParam);
        if(vals != null){
          for (String val : vals) {
            params.add(returnParam,val);
          }
        }

      }
    }

    if(Boolean.TRUE.equals( req.getParams().getBool("initArgs"))) rsp.add("initArgs", initArgs);
        
    // Write the streams...
    if( req.getContentStreams() != null ) {
      ArrayList<NamedList<Object>> streams = new ArrayList<>();
      // Cycle through each stream
      for( ContentStream content : req.getContentStreams() ) {
        NamedList<Object> stream = new SimpleOrderedMap<>();
        stream.add( "name", content.getName() );
        stream.add( "sourceInfo", content.getSourceInfo() );
        stream.add( "size", content.getSize() );
        stream.add( "contentType", content.getContentType() );
        Reader reader = content.getReader();
        try {
          stream.add( "stream", IOUtils.toString(reader) );
        } finally {
          reader.close();
        }
        streams.add( stream );
      }
      rsp.add( "streams", streams );
    }

    rsp.add("context", req.getContext());
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Dump handler (debug)";
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if(subpaths !=null && subpaths.contains(path)) return this;
    return null;
  }
  private List<String> subpaths;

  @Override
  public void init(NamedList args) {
    super.init(args);
    if(args !=null) {
      NamedList nl = (NamedList) args.get(PluginInfo.DEFAULTS);
      if(nl!=null) subpaths = nl.getAll("subpath");
    }
  }
}
