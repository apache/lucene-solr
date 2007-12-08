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

import java.io.IOException;
import java.net.URL;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * TODO!
 * 
 * @version $Id$
 * @since solr 1.3
 */
public abstract class SearchComponent implements SolrInfoMBean, NamedListInitializedPlugin
{
  public abstract void prepare( SolrQueryRequest req, SolrQueryResponse rsp ) throws IOException, ParseException;
  public abstract void process( SolrQueryRequest req, SolrQueryResponse rsp ) throws IOException;

  //////////////////////// NamedListInitializedPlugin methods //////////////////////
  
  public void init( NamedList args )
  {
    // By default do nothing
  }
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  public String getName() {
    return this.getClass().getName();
  }

  public abstract String getDescription();
  public abstract String getSourceId();
  public abstract String getSource();
  public abstract String getVersion();
  
  public Category getCategory() {
    return Category.OTHER;
  }

  public URL[] getDocs() {
    return null;  // this can be overridden, but not required
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    return lst;
  }
}
