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

package org.apache.solr.handler.admin;

import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * similar to "admin/registry.jsp" 
 * 
 * NOTE: the response format is still likely to change.  It should be designed so
 * that it works nicely with an XSLT transformation.  Until we have a nice
 * XSLT front end for /admin, the format is still open to change.
 * 
 *
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
    SimpleOrderedMap<Object> list = new SimpleOrderedMap<Object>();
    for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) 
    {
      SimpleOrderedMap<Object> category = new SimpleOrderedMap<Object>();
      list.add( cat.name(), category );
      Map<String, SolrInfoMBean> reg = core.getInfoRegistry();
      for (Map.Entry<String,SolrInfoMBean> entry : reg.entrySet()) {
        SolrInfoMBean m = entry.getValue();
        if (m.getCategory() != cat) continue;

        String na = "Not Declared";
        SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();
        category.add( entry.getKey(), info );

        info.add( "name",        (m.getName()       !=null ? m.getName()        : na) );
        info.add( "version",     (m.getVersion()    !=null ? m.getVersion()     : na) );
        info.add( "description", (m.getDescription()!=null ? m.getDescription() : na) );

        info.add( "sourceId",    (m.getSourceId()   !=null ? m.getSourceId()    : na) );
        info.add( "source",      (m.getSource()     !=null ? m.getSource()      : na) );

        URL[] urls = m.getDocs();
        if ((urls != null) && (urls.length > 0)) {
          ArrayList<String> docs = new ArrayList<String>(urls.length);
          for( URL u : urls ) {
            docs.add( u.toExternalForm() );
          }
          info.add( "docs", docs );
        }

        if( stats ) {
          info.add( "stats", m.getStatistics() );
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
}
