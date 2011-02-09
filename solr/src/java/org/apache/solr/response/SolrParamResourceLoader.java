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

package org.apache.solr.response;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.velocity.runtime.resource.loader.ResourceLoader;
import org.apache.velocity.runtime.resource.Resource;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.commons.collections.ExtendedProperties;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SolrParamResourceLoader extends ResourceLoader {
  private Map<String,String> templates = new HashMap<String,String>();
  public SolrParamResourceLoader(SolrQueryRequest request) {
    super();

    // TODO: Consider using content streams, but need a template name associated with each stream
    // for now, a custom param convention of template.<name>=<template body> is a nice example
    // of per-request overrides of templates

    org.apache.solr.common.params.SolrParams params = request.getParams();
    Iterator<String> names = params.getParameterNamesIterator();
    while (names.hasNext()) {
      String name = names.next();
      
      if (name.startsWith("v.template.")) {
        templates.put(name.substring(11) + ".vm",params.get(name));
      }
    }
  }

  @Override
  public void init(ExtendedProperties extendedProperties) {
  }

  @Override
  public InputStream getResourceStream(String s) throws ResourceNotFoundException {
    String template = templates.get(s);
    try {
      return template == null ? null : new ByteArrayInputStream(template.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e); // may not happen
    }
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
