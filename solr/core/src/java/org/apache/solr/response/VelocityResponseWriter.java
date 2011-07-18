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

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.*;

import java.io.*;
import java.util.Properties;

public class VelocityResponseWriter implements QueryResponseWriter {

  // TODO: maybe pass this Logger to the template for logging from there?
//  private static final Logger log = LoggerFactory.getLogger(VelocityResponseWriter.class);

  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    VelocityEngine engine = getEngine(request);  // TODO: have HTTP headers available for configuring engine

    Template template = getTemplate(engine, request);

    VelocityContext context = new VelocityContext();

    context.put("request", request);

    // Turn the SolrQueryResponse into a SolrResponse.
    // QueryResponse has lots of conveniences suitable for a view
    // Problem is, which SolrResponse class to use?
    // One patch to SOLR-620 solved this by passing in a class name as
    // as a parameter and using reflection and Solr's class loader to
    // create a new instance.  But for now the implementation simply
    // uses QueryResponse, and if it chokes in a known way, fall back
    // to bare bones SolrResponseBase.
    // TODO: Can this writer know what the handler class is?  With echoHandler=true it can get its string name at least
    SolrResponse rsp = new QueryResponse();
    NamedList<Object> parsedResponse = BinaryResponseWriter.getParsedResponse(request, response);
    try {
      rsp.setResponse(parsedResponse);

      // page only injected if QueryResponse works
      context.put("page", new PageTool(request, response));  // page tool only makes sense for a SearchHandler request... *sigh*
    } catch (ClassCastException e) {
      // known edge case where QueryResponse's extraction assumes "response" is a SolrDocumentList
      // (AnalysisRequestHandler emits a "response")
      e.printStackTrace();
      rsp = new SolrResponseBase();
      rsp.setResponse(parsedResponse);
    }
    context.put("response", rsp);

    // Velocity context tools - TODO: make these pluggable
    context.put("esc", new EscapeTool());
    context.put("date", new ComparisonDateTool());
    context.put("list", new ListTool());
    context.put("math", new MathTool());
    context.put("number", new NumberTool());
    context.put("sort", new SortTool());

    context.put("engine", engine);  // for $engine.resourceExists(...)

    String layout_template = request.getParams().get("v.layout");
    String json_wrapper = request.getParams().get("v.json");
    boolean wrap_response = (layout_template != null) || (json_wrapper != null);

    // create output, optionally wrap it into a json object
    if (wrap_response) {
      StringWriter stringWriter = new StringWriter();
      template.merge(context, stringWriter);

      if (layout_template != null) {
        context.put("content", stringWriter.toString());
        stringWriter = new StringWriter();
        try {
          engine.getTemplate(layout_template + ".vm").merge(context, stringWriter);
        } catch (Exception e) {
          throw new IOException(e.getMessage());
        }
      }

      if (json_wrapper != null) {
        writer.write(request.getParams().get("v.json") + "(");
        writer.write(getJSONWrap(stringWriter.toString()));
        writer.write(')');
      } else {  // using a layout, but not JSON wrapping
        writer.write(stringWriter.toString());
      }
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
    SolrVelocityResourceLoader resourceLoader =
        new SolrVelocityResourceLoader(request.getCore().getSolrConfig().getResourceLoader());
    engine.setProperty("solr.resource.loader.instance", resourceLoader);

    // TODO: Externalize Velocity properties
    engine.setProperty(VelocityEngine.RESOURCE_LOADER, "params,file,solr");
    String propFile = request.getParams().get("v.properties");
    try {
      if (propFile == null)
        engine.init();
      else {
        InputStream is = null;
        try {
          is = resourceLoader.getResourceStream(propFile);
          Properties props = new Properties();
          props.load(is);
          engine.init(props);
        }
        finally {
          if (is != null) is.close();
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return engine;
  }

  private Template getTemplate(VelocityEngine engine, SolrQueryRequest request) throws IOException {
    Template template;

    String template_name = request.getParams().get("v.template");
    String qt = request.getParams().get("qt");
    String path = (String) request.getContext().get("path");
    if (template_name == null && path != null) {
      template_name = path;
    }  // TODO: path is never null, so qt won't get picked up  maybe special case for '/select' to use qt, otherwise use path?
    if (template_name == null && qt != null) {
      template_name = qt;
    }
    if (template_name == null) template_name = "index";
    try {
      template = engine.getTemplate(template_name + ".vm");
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }

    return template;
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return request.getParams().get("v.contentType", "text/html;charset=UTF-8");
  }

  private String getJSONWrap(String xmlResult) {  // TODO: maybe noggit or Solr's JSON utilities can make this cleaner?
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
