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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.tools.generic.CollectionTool;
import org.apache.velocity.tools.generic.ComparisonDateTool;
import org.apache.velocity.tools.generic.DisplayTool;
import org.apache.velocity.tools.generic.EscapeTool;
import org.apache.velocity.tools.generic.LocaleConfig;
import org.apache.velocity.tools.generic.MathTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.apache.velocity.tools.generic.ResourceTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

@SuppressWarnings("rawtypes")
public class VelocityResponseWriter implements QueryResponseWriter, SolrCoreAware {
  // init param names, these are _only_ loaded at init time (no per-request control of these)
  //   - multiple different named writers could be created with different init params
  public static final String TEMPLATE_BASE_DIR = "template.base.dir";
  public static final String PARAMS_RESOURCE_LOADER_ENABLED = "params.resource.loader.enabled";
  public static final String SOLR_RESOURCE_LOADER_ENABLED = "solr.resource.loader.enabled";
  public static final String PROPERTIES_FILE = "init.properties.file";

  // request param names
  public static final String TEMPLATE = "v.template";
  public static final String LAYOUT = "v.layout";
  public static final String LAYOUT_ENABLED = "v.layout.enabled";
  public static final String CONTENT_TYPE = "v.contentType";
  public static final String JSON = "v.json";
  public static final String LOCALE = "v.locale";

  public static final String TEMPLATE_EXTENSION = ".vm";
  public static final String DEFAULT_CONTENT_TYPE = "text/html;charset=UTF-8";
  public static final String JSON_CONTENT_TYPE = "application/json;charset=UTF-8";

  private File fileResourceLoaderBaseDir;
  private boolean paramsResourceLoaderEnabled;
  private boolean solrResourceLoaderEnabled;
  private String initPropertiesFileName;  // used just to hold from init() to inform()

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Properties velocityInitProps = new Properties();
  private Map<String,String> customTools = new HashMap<String,String>();

  @Override
  public void init(NamedList args) {
    fileResourceLoaderBaseDir = null;
    String templateBaseDir = (String) args.get(TEMPLATE_BASE_DIR);

    if (templateBaseDir != null && !templateBaseDir.isEmpty()) {
      fileResourceLoaderBaseDir = new File(templateBaseDir).getAbsoluteFile();
      if (!fileResourceLoaderBaseDir.exists()) { // "*not* exists" condition!
        log.warn(TEMPLATE_BASE_DIR + " specified does not exist: " + fileResourceLoaderBaseDir);
        fileResourceLoaderBaseDir = null;
      } else {
        if (!fileResourceLoaderBaseDir.isDirectory()) { // "*not* a directory" condition
          log.warn(TEMPLATE_BASE_DIR + " specified is not a directory: " + fileResourceLoaderBaseDir);
          fileResourceLoaderBaseDir = null;
        }
      }
    }

    // params resource loader: off by default
    Boolean prle = args.getBooleanArg(PARAMS_RESOURCE_LOADER_ENABLED);
    paramsResourceLoaderEnabled = (null == prle ? false : prle);

    // solr resource loader: on by default
    Boolean srle = args.getBooleanArg(SOLR_RESOURCE_LOADER_ENABLED);
    solrResourceLoaderEnabled = (null == srle ? true : srle);

    initPropertiesFileName = (String) args.get(PROPERTIES_FILE);

    NamedList tools = (NamedList)args.get("tools");
    if (tools != null) {
      for(Object t : tools) {
        Map.Entry tool = (Map.Entry)t;
        customTools.put(tool.getKey().toString(), tool.getValue().toString());
      }
    }
  }

  @Override
  public void inform(SolrCore core) {
    // need to leverage SolrResourceLoader, so load init.properties.file here instead of init()
    if (initPropertiesFileName != null) {
      try {
        velocityInitProps.load(new InputStreamReader(core.getResourceLoader().openResource(initPropertiesFileName), StandardCharsets.UTF_8));
      } catch (IOException e) {
        log.warn("Error loading " + PROPERTIES_FILE + " specified property file: " + initPropertiesFileName, e);
      }
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    String contentType = request.getParams().get(CONTENT_TYPE);

    // Use the v.contentType specified, or either of the default content types depending on the presence of v.json
    return (contentType != null) ? contentType : ((request.getParams().get(JSON) == null) ? DEFAULT_CONTENT_TYPE : JSON_CONTENT_TYPE);
  }

  @Override
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    VelocityEngine engine = createEngine(request);  // TODO: have HTTP headers available for configuring engine

    Template template = getTemplate(engine, request);

    VelocityContext context = createContext(request, response);
    context.put("engine", engine);  // for $engine.resourceExists(...)

    String layoutTemplate = request.getParams().get(LAYOUT);
    boolean layoutEnabled = request.getParams().getBool(LAYOUT_ENABLED, true) && layoutTemplate != null;

    String jsonWrapper = request.getParams().get(JSON);
    boolean wrapResponse = layoutEnabled || jsonWrapper != null;

    // create output
    if (!wrapResponse) {
      // straight-forward template/context merge to output
      template.merge(context, writer);
    }
    else {
      // merge to a string buffer, then wrap with layout and finally as JSON
      StringWriter stringWriter = new StringWriter();
      template.merge(context, stringWriter);

      if (layoutEnabled) {
        context.put("content", stringWriter.toString());
        stringWriter = new StringWriter();
        try {
          engine.getTemplate(layoutTemplate + TEMPLATE_EXTENSION).merge(context, stringWriter);
        } catch (Exception e) {
          throw new IOException(e.getMessage());
        }
      }

      if (jsonWrapper != null) {
        for (int i=0; i<jsonWrapper.length(); i++) {
          if (!Character.isJavaIdentifierPart(jsonWrapper.charAt(i))) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid function name for " + JSON + ": '" + jsonWrapper + "'");
          }
        }
        writer.write(jsonWrapper + "(");
        writer.write(getJSONWrap(stringWriter.toString()));
        writer.write(')');
      } else {  // using a layout, but not JSON wrapping
        writer.write(stringWriter.toString());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private VelocityContext createContext(SolrQueryRequest request, SolrQueryResponse response) {
    VelocityContext context = new VelocityContext();

    // Register useful Velocity "tools"
    String locale = request.getParams().get(LOCALE);
    Map toolConfig = new HashMap();
    toolConfig.put("locale", locale);


    context.put("log", log);   // TODO: add test; TODO: should this be overridable with a custom "log" named tool?
    context.put("esc", new EscapeTool());
    context.put("date", new ComparisonDateTool());
    context.put(SORT, new CollectionTool());

    MathTool mathTool = new MathTool();
    mathTool.configure(toolConfig);
    context.put("math", mathTool);

    NumberTool numberTool = new NumberTool();
    numberTool.configure(toolConfig);
    context.put("number", numberTool);


    DisplayTool displayTool = new DisplayTool();
    displayTool.configure(toolConfig);
    context.put("display", displayTool);

    ResourceTool resourceTool = new SolrVelocityResourceTool(request.getCore().getSolrConfig().getResourceLoader().getClassLoader());
    resourceTool.configure(toolConfig);
    context.put("resource", resourceTool);

/*
    // Custom tools, specified in config as:
        <queryResponseWriter name="velocityWithCustomTools" class="solr.VelocityResponseWriter">
          <lst name="tools">
            <str name="mytool">com.example.solr.velocity.MyTool</str>
          </lst>
        </queryResponseWriter>
*/
    // Custom tools can override any of the built-in tools provided above, by registering one with the same name
    for(Map.Entry<String, String> entry : customTools.entrySet()) {
      String name = entry.getKey();
      Object customTool = SolrCore.createInstance(entry.getValue(), Object.class, "VrW custom tool: " + name, request.getCore(), request.getCore().getResourceLoader());
      if (customTool instanceof LocaleConfig) {
        ((LocaleConfig)customTool).configure(toolConfig);
      }
      context.put(name, customTool);
    }

    // custom tools _cannot_ override context objects added below, like $request and $response
    // TODO: at least log a warning when one of the *fixed* tools classes in name with a custom one, currently silently ignored


    // Turn the SolrQueryResponse into a SolrResponse.
    // QueryResponse has lots of conveniences suitable for a view
    // Problem is, which SolrResponse class to use?
    // One patch to SOLR-620 solved this by passing in a class name as
    // as a parameter and using reflection and Solr's class loader to
    // create a new instance.  But for now the implementation simply
    // uses QueryResponse, and if it chokes in a known way, fall back
    // to bare bones SolrResponseBase.
    // Can this writer know what the handler class is?  With echoHandler=true it can get its string name at least
    SolrResponse rsp = new QueryResponse();
    NamedList<Object> parsedResponse = BinaryResponseWriter.getParsedResponse(request, response);
    try {
      rsp.setResponse(parsedResponse);

      // page only injected if QueryResponse works
      context.put("page", new PageTool(request, response));  // page tool only makes sense for a SearchHandler request
      context.put("debug",((QueryResponse)rsp).getDebugMap());
    } catch (ClassCastException e) {
      // known edge case where QueryResponse's extraction assumes "response" is a SolrDocumentList
      // (AnalysisRequestHandler emits a "response")
      rsp = new SolrResponseBase();
      rsp.setResponse(parsedResponse);
    }

    context.put("request", request);
    context.put("response", rsp);

    return context;
  }

  private VelocityEngine createEngine(SolrQueryRequest request) {
    VelocityEngine engine = new VelocityEngine();

    // Set some engine properties that improve the experience
    //   - these could be considered in the future for parameterization, but can also be overridden by using
    //     the init.properties.file setting.  (TODO: add a test for this properties set here overridden)

    // load the built-in _macros.vm first, then load VM_global_library.vm for legacy (pre-5.0) support,
    // and finally allow macros.vm to have the final say and override anything defined in the preceding files.
    engine.setProperty(RuntimeConstants.VM_LIBRARY, "_macros.vm,VM_global_library.vm,macros.vm");

    // Standard templates autoload, but not the macro one(s), by default, so let's just make life
    // easier, and consistent, for macro development too.
    engine.setProperty(RuntimeConstants.VM_LIBRARY_AUTORELOAD, "true");

    /*
      Set up Velocity resource loader(s)
       terminology note: "resource loader" is overloaded here, there is Solr's resource loader facility for plugins,
       and there are Velocity template resource loaders.  It's confusing, they overlap: there is a Velocity resource
       loader that loads templates from Solr's resource loader (SolrVelocityResourceLoader).

      The Velocity resource loader order is [params,][file,][solr], intentionally ordered in this manner, and each
      one optional and individually enable-able.  By default, only "solr" (resource loader) is used, parsing templates
      from a velocity/ sub-tree in either the classpath or under conf/.

      A common usage would be to enable the file template loader, keeping the solr loader enabled; the Velocity resource
      loader path would then be "file,solr" (params is disabled by default).  The basic browse templates are built into
      this plugin, but can be individually overridden by placing a same-named template in the template.base.dir specified
      directory.
     */
    ArrayList<String> loaders = new ArrayList<String>();
    if (paramsResourceLoaderEnabled) {
      loaders.add("params");
      engine.setProperty("params.resource.loader.instance", new SolrParamResourceLoader(request));
    }
    if (fileResourceLoaderBaseDir != null) {
      loaders.add("file");
      engine.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, fileResourceLoaderBaseDir.getAbsolutePath());
    }
    if (solrResourceLoaderEnabled) {
      // The solr resource loader serves templates under a velocity/ subtree from <lib>, conf/,
      // or SolrCloud's configuration tree.  Or rather the other way around, other resource loaders are rooted
      // from the top, whereas this is velocity/ sub-tree rooted.
      loaders.add("solr");
      engine.setProperty("solr.resource.loader.instance", new SolrVelocityResourceLoader(request.getCore().getSolrConfig().getResourceLoader()));
    }

    // Always have the built-in classpath loader.  This is needed when using VM_LIBRARY macros, as they are required
    // to be present if specified, and we want to have a nice macros facility built-in for users to use easily, and to
    // extend in custom ways.
    loaders.add("builtin");
    engine.setProperty("builtin.resource.loader.instance", new ClasspathResourceLoader());

    engine.setProperty(RuntimeConstants.RESOURCE_LOADER, String.join(",", loaders));

    engine.setProperty(RuntimeConstants.INPUT_ENCODING, "UTF-8");

    // Work around VELOCITY-908 with Velocity not handling locales properly
    Object spaceGobblingInitProperty = velocityInitProps.get(RuntimeConstants.SPACE_GOBBLING);
    if(spaceGobblingInitProperty != null) {
      // If there is an init property, uppercase it before Velocity.
      velocityInitProps.put(RuntimeConstants.SPACE_GOBBLING,
          String.valueOf(spaceGobblingInitProperty).toUpperCase(Locale.ROOT));
    } else {
      // Fallback to checking if the engine default property is set and if not make it a reasonable default.
      Object spaceGobblingEngineProperty = engine.getProperty(RuntimeConstants.SPACE_GOBBLING);
      if(spaceGobblingEngineProperty == null) {
        engine.setProperty(RuntimeConstants.SPACE_GOBBLING, RuntimeConstants.SpaceGobbling.LINES.toString());
      }
    }

    // bring in any custom properties too
    engine.init(velocityInitProps);

    return engine;
  }

  private Template getTemplate(VelocityEngine engine, SolrQueryRequest request) throws IOException {
    Template template;

    String templateName = request.getParams().get(TEMPLATE);

    String qt = request.getParams().get(CommonParams.QT);
    String path = (String) request.getContext().get("path");
    if (templateName == null && path != null) {
      templateName = path;
    }  // TODO: path is never null, so qt won't get picked up  maybe special case for '/select' to use qt, otherwise use path?
    if (templateName == null && qt != null) {
      templateName = qt;
    }
    if (templateName == null) templateName = "index";
    try {
      template = engine.getTemplate(templateName + TEMPLATE_EXTENSION);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }

    return template;
  }

  private String getJSONWrap(String xmlResult) {  // maybe noggit or Solr's JSON utilities can make this cleaner?
    // escape the double quotes and backslashes
    String replace1 = xmlResult.replaceAll("\\\\", "\\\\\\\\");
    replace1 = replace1.replaceAll("\\n", "\\\\n");
    replace1 = replace1.replaceAll("\\r", "\\\\r");
    String replaced = replace1.replaceAll("\"", "\\\\\"");
    // wrap it in a JSON object
    return "{\"result\":\"" + replaced + "\"}";
  }

  // see: https://github.com/apache/velocity-tools/blob/trunk/velocity-tools-generic/src/main/java/org/apache/velocity/tools/generic/ResourceTool.java
  private static class SolrVelocityResourceTool extends ResourceTool {

    private ClassLoader solrClassLoader;

    public SolrVelocityResourceTool(ClassLoader cl) {
      this.solrClassLoader = cl;
    }

    @Override
    protected ResourceBundle getBundle(String baseName, Object loc) {
      // resource bundles for this tool must be in velocity "package"
      return ResourceBundle.getBundle(
          "velocity." + baseName,
          (loc == null) ? this.getLocale() : this.toLocale(loc),
          solrClassLoader);
    }
  }
}
