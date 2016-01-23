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

package org.apache.solr.highlight;

import java.net.URL;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoMBean;

/**
 * 
 * @since solr 1.3
 */
public abstract class HighlightingPluginBase implements SolrInfoMBean
{
  protected long numRequests;
  protected SolrParams defaults;

  public void init(NamedList args) {
    if( args != null ) {
      Object o = args.get("defaults");
      if (o != null && o instanceof NamedList ) {
        defaults = SolrParams.toSolrParams((NamedList)o);
      }
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public abstract String getDescription();
  @Override
  public String getSource() { return null; }
  
  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }
  
  @Override
  public Category getCategory()
  {
    return Category.HIGHLIGHTING;
  }

  @Override
  public URL[] getDocs() {
    return null;  // this can be overridden, but not required
  }

  @Override
  public NamedList getStatistics() {
    NamedList<Long> lst = new SimpleOrderedMap<>();
    lst.add("requests", numRequests);
    return lst;
  }
}


