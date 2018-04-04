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

import java.io.IOException;
import java.util.Enumeration;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.RedactionUtils;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 *
 * @since solr 1.2
 */
public class PropertiesRequestHandler extends RequestHandlerBase
{

  public static final String REDACT_STRING = RedactionUtils.getRedactString();

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    NamedList<String> props = new SimpleOrderedMap<>();
    String name = req.getParams().get(NAME);
    if( name != null ) {
      String property = getSecuredPropertyValue(name);
      props.add( name, property);
    }
    else {
      Enumeration<?> enumeration = System.getProperties().propertyNames();
      while(enumeration.hasMoreElements()){
        name = (String) enumeration.nextElement();
        props.add(name, getSecuredPropertyValue(name));
      }
    }
    rsp.add( "system.properties", props );
    rsp.setHttpCaching(false);
  }

  private String getSecuredPropertyValue(String name) {
    if(RedactionUtils.isSystemPropertySensitive(name)){
      return REDACT_STRING;
    }
    return System.getProperty(name);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Get System Properties";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
