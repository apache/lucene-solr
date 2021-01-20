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
package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.*;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An update request processor factory that enables the use of update 
 * processors implemented as scripts which can be loaded by the 
 * {@link SolrResourceLoader} (usually via the <code>conf</code> dir for 
 * the SolrCore).
 * </p>
 * <p>
 * This factory requires at least one configuration parameter named
 * <code>script</code> which may be the name of a script file as a string, 
 * or an array of multiple script files.  If multiple script files are 
 * specified, they are executed sequentially in the order specified in the 
 * configuration -- as if multiple factories were configured sequentially
 * </p>
 * <p>
 * Each script file is expected to declare functions with the same name 
 * as each method in {@link UpdateRequestProcessor}, using the same 
 * arguments.  One slight deviation is in the optional return value from 
 * these functions: If a script function has a <code>boolean</code> return 
 * value, and that value is <code>false</code> then the processor will 
 * cleanly terminate processing of the command and return, without forwarding 
 * the command on to the next script or processor in the chain.
 * Due to limitations in the {@link ScriptEngine} API used by 
 * this factory, it can not enforce that all functions exist on initialization,
 * so errors from missing functions will only be generated at runtime when
 * the chain attempts to use them.
 * </p>
 * <p>
 * The factory may also be configured with an optional "params" argument, 
 * which can be an {@link NamedList} (or array, or any other simple Java 
 * object) which will be put into the global scope for each script.
 * </p>
 * <p>
 * The following variables are define as global variables for each script:
 * <ul>
 *  <li>req - The {@link SolrQueryRequest}</li>
 *  <li>rsp - The {@link SolrQueryResponse}</li>
 *  <li>logger - A {@link Logger} that can be used for logging purposes in the script</li>
 *  <li>params - The "params" init argument in the factory configuration (if any)</li>
 * </ul>
 * <p>
 * Internally this update processor uses JDK 6 scripting engine support, 
 * and any {@link Invocable} implementations of <code>ScriptEngine</code> 
 * that can be loaded using the Solr Plugin ClassLoader may be used.  
 * By default, the engine used for each script is determined by the filed 
 * extension (ie: a *.js file will be treated as a JavaScript script) but 
 * this can be overridden by specifying an explicit "engine" name init 
 * param for the factory, which identifies a registered name of a 
 * {@link ScriptEngineFactory}. 
 * (This may be particularly useful if multiple engines are available for 
 * the same scripting language, and you wish to force the usage of a 
 * particular engine because of known quirks)
 * </p>
 * <p>
 * A new {@link ScriptEngineManager} is created for each 
 * <code>SolrQueryRequest</code> defining a "global" scope for the script(s) 
 * which is request specific.  Separate <code>ScriptEngine</code> instances 
 * are then used to evaluate the script files, resulting in an "engine" scope 
 * that is specific to each script.
 * </p>
 * <p>
 * A simple example...
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.StatelessScriptUpdateProcessorFactory"&gt;
 *   &lt;str name="script"&gt;updateProcessor.js&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * <p>
 * A more complex example involving multiple scripts in different languages, 
 * and a "params" <code>NamedList</code> that will be put into the global 
 * scope of each script...
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.StatelessScriptUpdateProcessorFactory"&gt;
 *   &lt;arr name="script"&gt;
 *     &lt;str name="script"&gt;first-processor.js&lt;/str&gt;
 *     &lt;str name="script"&gt;second-processor.py&lt;/str&gt;
 *   &lt;/arr&gt;
 *   &lt;lst name="params"&gt;
 *     &lt;bool name="a_bool_value"&gt;true&lt;/bool&gt;
 *     &lt;int name="and_int_value"&gt;3&lt;/int&gt;
 *   &lt;/lst&gt;
 * &lt;/processor&gt;
 * </pre>
 * <p>
 * An example where the script file extensions are ignored, and an 
 * explicit script engine is used....
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.StatelessScriptUpdateProcessorFactory"&gt;
 *   &lt;arr name="script"&gt;
 *     &lt;str name="script"&gt;first-processor.txt&lt;/str&gt;
 *     &lt;str name="script"&gt;second-processor.txt&lt;/str&gt;
 *   &lt;/arr&gt;
 *   &lt;str name="engine"&gt;rhino&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * 
 * @since 4.0.0
 */
public class StatelessScriptUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final static String SCRIPT_ARG = "script";
  private final static String PARAMS_ARG = "params";
  private final static String ENGINE_NAME_ARG = "engine";

  private List<ScriptFile> scriptFiles;

  /** if non null, this is an override for the engine for all scripts */
  private String engineName = null;

  private Object params = null;

  private SolrResourceLoader resourceLoader;

  private ScriptEngineCustomizer scriptEngineCustomizer;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    @SuppressWarnings({"unchecked"})
    Collection<String> scripts =
      args.removeConfigArgs(SCRIPT_ARG);
    if (scripts.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                              "StatelessScriptUpdateProcessorFactory must be " +
                              "initialized with at least one " + SCRIPT_ARG);
    }
    scriptFiles = new ArrayList<>();
    for (String script : scripts) {
      scriptFiles.add(new ScriptFile(script));
    }

    params = args.remove(PARAMS_ARG);

    Object engine = args.remove(ENGINE_NAME_ARG);
    if (engine != null) {
      if (engine instanceof String) {
        engineName = (String)engine;
      } else {
        throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR, 
           "'" + ENGINE_NAME_ARG + "' init param must be a String (found: " + 
           engine.getClass() + ")");
      }
    }

    super.init(args);

  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    List<EngineInfo> scriptEngines = null;

    scriptEngines = initEngines(req, rsp);

    return new ScriptUpdateProcessor(req, rsp, scriptEngines, next);
  }

  // TODO: Make this useful outside of tests, such that a ScriptEngineCustomizer could be looked up through the resource loader
  void setScriptEngineCustomizer(ScriptEngineCustomizer scriptEngineCustomizer) {
    this.scriptEngineCustomizer = scriptEngineCustomizer;
  }

  @Override
  public void inform(SolrCore core) {
    if (!core.getCoreDescriptor().isConfigSetTrusted()) {
      throw new SolrException(ErrorCode.UNAUTHORIZED, "The configset for this collection was uploaded without any authentication in place,"
          + " and this operation is not available for collections with untrusted configsets. To use this component, re-upload the configset"
          + " after enabling authentication and authorization.");
    }
    resourceLoader = core.getResourceLoader();

    // test that our engines & scripts are valid

    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    try {
      initEngines(req, rsp);
    } catch (Exception e) {
      String msg = "Unable to initialize scripts: " + e.getMessage();
      log.error(msg, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
    } finally {
      req.close();
    }

    
  }


  //================================================ Helper Methods ==================================================

  /**
   * Initializes a list of script engines - an engine per script file.
   *
   * @param req The solr request.
   * @param rsp The solr response
   * @return The list of initialized script engines.
   */
  private List<EngineInfo> initEngines(SolrQueryRequest req, 
                                       SolrQueryResponse rsp) 
    throws SolrException {
    
    List<EngineInfo> scriptEngines = new ArrayList<>();

    ScriptEngineManager scriptEngineManager 
      = new ScriptEngineManager(resourceLoader.getClassLoader());

    scriptEngineManager.put("logger", log);
    scriptEngineManager.put("req", req);
    scriptEngineManager.put("rsp", rsp);
    if (params != null) {
      scriptEngineManager.put("params", params);
    }

    for (ScriptFile scriptFile : scriptFiles) {
      final ScriptEngine engine;
      if (null != engineName) {
        engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
          String details = getSupportedEngines(scriptEngineManager, false);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                  "No ScriptEngine found by name: "
                                  + engineName + 
                                  (null != details ? 
                                   " -- supported names: " + details : ""));
        }
      } else {
        engine = scriptEngineManager.getEngineByExtension
          (scriptFile.getExtension());
        if (engine == null) {
          String details = getSupportedEngines(scriptEngineManager, true);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                  "No ScriptEngine found by file extension: "
                                  + scriptFile.getFileName() + 
                                  (null != details ? 
                                   " -- supported extensions: " + details : ""));
                                  
        }
      }

      if (! (engine instanceof Invocable)) {
        String msg = 
          "Engine " + ((null != engineName) ? engineName : 
                       ("for script " + scriptFile.getFileName())) +
          " does not support function invocation (via Invocable): " +
          engine.getClass().toString() + " (" +
          engine.getFactory().getEngineName() + ")";
        log.error(msg);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      }

      if (scriptEngineCustomizer != null) {
        scriptEngineCustomizer.customize(engine);
      }

      scriptEngines.add(new EngineInfo((Invocable)engine, scriptFile));
      try {
        Reader scriptSrc = scriptFile.openReader(resourceLoader);
  
        try {
          try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws ScriptException  {
                engine.eval(scriptSrc);
                return null;
              }
            }, SCRIPT_SANDBOX);
          } catch (PrivilegedActionException e) {
            throw (ScriptException) e.getException();
          }
        } catch (ScriptException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                  "Unable to evaluate script: " + 
                                  scriptFile.getFileName(), e);
        } finally {
          IOUtils.closeQuietly(scriptSrc);
        }
      } catch (IOException ioe) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Unable to evaluate script: " + 
            scriptFile.getFileName(), ioe);        
      }
    }
    return scriptEngines;
  }

  /**
   * For error messages - returns null if there are any exceptions of any 
   * kind building the string (or of the list is empty for some unknown reason).
   * @param ext - if true, list of extensions, otherwise a list of engine names
   */
  private static String getSupportedEngines(ScriptEngineManager mgr,
                                            boolean ext) {
    String result = null;
    try {
      List<ScriptEngineFactory> factories = mgr.getEngineFactories();
      if (null == factories) return result;

      Set<String> engines = new LinkedHashSet<>(factories.size());
      for (ScriptEngineFactory f : factories) {
        if (ext) {
          engines.addAll(f.getExtensions());
        } else {
          engines.addAll(f.getNames());
        }
      }
      result = String.join(", ", engines);
    } catch (RuntimeException e) {
      /* :NOOP: */
    }
    return result;
  }



  //================================================= Inner Classes ==================================================

  /**
   * The actual update processor. All methods delegate to scripts.
   */
  private static class ScriptUpdateProcessor extends UpdateRequestProcessor {

    private List<EngineInfo> engines;

    private ScriptUpdateProcessor(SolrQueryRequest req, SolrQueryResponse res, List<EngineInfo> engines, UpdateRequestProcessor next) {
      super(next);
      this.engines = engines;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (invokeFunction("processAdd", cmd)) {
        super.processAdd(cmd);
      }
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      if (invokeFunction("processDelete", cmd)) {
        super.processDelete(cmd);
      }
        
    }

    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      if (invokeFunction("processMergeIndexes", cmd)) {
        super.processMergeIndexes(cmd);
      }
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      if (invokeFunction("processCommit", cmd)) {
        super.processCommit(cmd);
      }
    }

    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      if (invokeFunction("processRollback", cmd)) {
        super.processRollback(cmd);
      }
    }

    @Override
    public void finish() throws IOException {
      if (invokeFunction("finish")) {
        super.finish();
      }
    }

    /**
     * returns true if processing should continue, or false if the 
     * request should be ended now.  Result value is computed from the return 
     * value of the script function if: it exists, is non-null, and can be 
     * cast to a java Boolean.
     */
    private boolean invokeFunction(String name, Object... cmd) {
      return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          return invokeFunctionUnsafe(name, cmd);
        }
      }, SCRIPT_SANDBOX);
    }

    private boolean invokeFunctionUnsafe(String name, Object... cmd) {

      for (EngineInfo engine : engines) {
        try {
          Object result = engine.getEngine().invokeFunction(name, cmd);
          if (null != result && result instanceof Boolean) {
            if (! ((Boolean)result).booleanValue() ) {
              return false;
            }
          }

        } catch (ScriptException | NoSuchMethodException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                                  "Unable to invoke function " + name + 
                                  " in script: " + 
                                  engine.getScriptFile().getFileName() + 
                                  ": " + e.getMessage(), e);
        }
      }

      return true;
    }
  }

  /**
   * Holds the script engine and its associated script file.
   */
  private static class EngineInfo {

    private final Invocable engine;
    private final ScriptFile scriptFile;

    private EngineInfo(Invocable engine, ScriptFile scriptFile) {
      this.engine = engine;
      this.scriptFile = scriptFile;
    }

    public Invocable getEngine() {
      return engine;
    }

    public ScriptFile getScriptFile() {
      return scriptFile;
    }
  }

  /**
   * Represents a script file.
   */
  private static class ScriptFile {

    private final String fileName;
    private final String extension;

    private ScriptFile(String fileName) {
      this.fileName = fileName;
      this.extension = FilenameUtils.getExtension(fileName);
    }

    public String getFileName() {
      return fileName;
    }

    public String getExtension() {
      return extension;
    }

    public Reader openReader(SolrResourceLoader resourceLoader) throws IOException {
      InputStream input = resourceLoader.openResource(fileName);
      return org.apache.lucene.util.IOUtils.getDecodingReader
        (input, StandardCharsets.UTF_8);
    }
  }

  // sandbox for script code: zero permissions
  private static final AccessControlContext SCRIPT_SANDBOX =
      new AccessControlContext(new ProtectionDomain[] { new ProtectionDomain(null, null) });
}
