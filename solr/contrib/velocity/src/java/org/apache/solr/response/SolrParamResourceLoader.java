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
package org.apache.solr.response;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.velocity.runtime.resource.loader.ResourceLoader;
import org.apache.velocity.runtime.resource.Resource;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.util.ExtProperties;

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SolrParamResourceLoader extends ResourceLoader {
  public static final String TEMPLATE_PARAM_PREFIX = VelocityResponseWriter.TEMPLATE + ".";

  private Map<String,String> templates = new HashMap<>();
  public SolrParamResourceLoader(SolrQueryRequest request) {
    super();

    // TODO: Consider using content streams, but need a template name associated with each stream
    // for now, a custom param convention of template.<name>=<template body> is a nice example
    // of per-request overrides of templates

    SolrParams params = request.getParams();
    Iterator<String> names = params.getParameterNamesIterator();
    while (names.hasNext()) {
      String name = names.next();
      
      if (name.startsWith(TEMPLATE_PARAM_PREFIX)) {
        templates.put(name.substring(TEMPLATE_PARAM_PREFIX.length()) + VelocityResponseWriter.TEMPLATE_EXTENSION,params.get(name));
      }
    }
  }

  @Override
  public void init(ExtProperties extendedProperties) {
  }

  @Override
  public Reader getResourceReader(String source, String encoding) throws ResourceNotFoundException {
    String template = templates.get(source);
    return template == null ? null : new StringReader(template);
  }

  @Override
  public boolean isSourceModified(Resource resource) {
    return false;
  }

  @Override
  public long getLastModified(Resource resource) {
    return 0;
  }
}
