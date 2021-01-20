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
package org.apache.solr.handler.admin;

import java.util.Map;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * @since solr 1.2
 */
public class PluginInfoHandler extends RequestHandlerBase
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    SolrParams params = req.getParams();
    
    boolean stats = params.getBool( "stats", false );
    rsp.add( "plugins", getSolrInfoBeans( req.getCore(), stats ) );
    rsp.setHttpCaching(false);
  }
  
  private static SimpleOrderedMap<Object> getSolrInfoBeans( SolrCore core, boolean stats )
  {
    SimpleOrderedMap<Object> list = new SimpleOrderedMap<>();
    for (SolrInfoBean.Category cat : SolrInfoBean.Category.values())
    {
      SimpleOrderedMap<Object> category = new SimpleOrderedMap<>();
      list.add( cat.name(), category );
      Map<String, SolrInfoBean> reg = core.getInfoRegistry();
      for (Map.Entry<String,SolrInfoBean> entry : reg.entrySet()) {
        SolrInfoBean m = entry.getValue();
        if (m.getCategory() != cat) continue;

        String na = "Not Declared";
        SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
        category.add( entry.getKey(), info );

        info.add( NAME,          (m.getName()       !=null ? m.getName()        : na) );
        info.add( "description", (m.getDescription()!=null ? m.getDescription() : na) );

        if (stats && m.getSolrMetricsContext() != null) {
          info.add( "stats", m.getSolrMetricsContext().getMetricsSnapshot());
        }
      }
    }
    return list;
  }
  
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Registry";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
