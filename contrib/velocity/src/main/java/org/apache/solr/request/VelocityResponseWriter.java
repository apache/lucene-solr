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

package org.apache.solr.request;

import org.apache.solr.common.util.NamedList;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.tools.generic.EscapeTool;
import org.apache.velocity.app.VelocityEngine;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class VelocityResponseWriter implements QueryResponseWriter {
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    VelocityEngine engine = getEngine(request);  // TODO: have HTTP headers available for configuring engine

    // TODO: Add layout capability, render to string buffer, then render layout
    Template template = getTemplate(engine, request);

    VelocityContext context = new VelocityContext();

    // TODO: Make this use the SolrJ API, rather than "embedded" Solr API
    context.put("request", request);    // TODO: inject a SolrRequest instead of a SolrQueryRequest
    context.put("response", response);  // TODO: inject a SolrResponse instead of a SolrQueryResponse
    context.put("page",new PageTool(request,response));
    context.put("esc", new EscapeTool());
   
    // create output, optionally wrap it into a json object
    if (request.getParams().get("v.json") != null) {
      StringWriter stringWriter = new StringWriter();
      template.merge(context, stringWriter);
      writer.write(request.getParams().get("v.json") + "(");
      writer.write(getJSONWrap(stringWriter.toString()));
      writer.write(')');
    } else {
      template.merge(context, writer);
    }
  }

  private VelocityEngine getEngine(SolrQueryRequest request) {
    VelocityEngine engine = new VelocityEngine();
    String template_root = request.getParams().get("v.base_dir");
    File baseDir = new File(request.getCore().getResourceLoader().getConfigDir(), "velocity");
    if (template_root != null) {
      baseDir = new File(template_root);
    }
    engine.setProperty(VelocityEngine.FILE_RESOURCE_LOADER_PATH, baseDir.getAbsolutePath());
    engine.setProperty("params.resource.loader.instance", new SolrParamResourceLoader(request));
    engine.setProperty("solr.resource.loader.instance",
        new SolrVelocityResourceLoader(request.getCore().getSolrConfig().getResourceLoader()));
    engine.setProperty(VelocityEngine.RESOURCE_LOADER, "params,file,solr");

    return engine;
  }

  private Template getTemplate(VelocityEngine engine, SolrQueryRequest request) throws IOException {
    Template template;
    try {
      template = engine.getTemplate(request.getParams().get("v.template", "browse") + ".vm");
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }

    return template;
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return request.getParams().get("v.contentType","text/html");
  }
  
  private String getJSONWrap(String xmlResult) {
    // escape the double quotes and backslashes
    String replace1 = xmlResult.replaceAll("\\\\", "\\\\\\\\");
    replace1 = replace1.replaceAll("\\n", "\\\\n");
    replace1 = replace1.replaceAll("\\r", "\\\\r");
    String replaced = replace1.replaceAll("\"", "\\\\\"");
    // wrap it in a JSON object
    return "{\"result\":\"" + replaced + "\"}";
  }

  public void init(NamedList args) {
    
  }
}
