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

package org.apache.solr.scripting.xslt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.handler.loader.XMLLoader;

/**
 * Add XML formatted documents to Solr, transforming them to the Solr XML format using 
 * a XSLT stylesheet via the 'tr' parameter.
 * 
 * use {@link UpdateRequestHandler}
 */

public class XsltUpdateRequestHandler extends UpdateRequestHandler {

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    setAssumeContentType("application/xml");
    loaders = Collections.unmodifiableMap(createXSLTLoader(args));

  }

  protected Map<String,ContentStreamLoader> createXSLTLoader(@SuppressWarnings({"rawtypes"})NamedList args) {

    SolrParams p = null;
    if(args!=null) {
      p = args.toSolrParams();
    }    
    Map<String,ContentStreamLoader> registry = new HashMap<>();

    boolean allowTransforms = true;
    registry.put("application/xml", new XMLLoader(allowTransforms).init(p) );
    registry.put("text/xml", registry.get("application/xml") );
    
    return registry;
  }  

  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getDescription() {
    return "Add documents with XML, transforming with XSLT first";
  }
}
