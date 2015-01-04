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

package org.apache.solr.core;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.BasicPermission;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;
import java.security.cert.Certificate;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public final class RequestHandlers {
  public static Logger log = LoggerFactory.getLogger(RequestHandlers.class);

  protected final SolrCore core;
  // Use a synchronized map - since the handlers can be changed at runtime, 
  // the map implementation should be thread safe
  private final Map<String, SolrRequestHandler> handlers =
      new ConcurrentHashMap<>() ;
  private final Map<String, SolrRequestHandler> immutableHandlers = Collections.unmodifiableMap(handlers) ;

  public static final boolean disableExternalLib = Boolean.parseBoolean(System.getProperty("disable.external.lib", "false"));

  /**
   * Trim the trailing '/' if it's there, and convert null to empty string.
   * 
   * we want:
   *  /update/csv   and
   *  /update/csv/
   * to map to the same handler 
   * 
   */
  public static String normalize( String p )
  {
    if(p == null) return "";
    if( p.endsWith( "/" ) && p.length() > 1 )
      return p.substring( 0, p.length()-1 );
    
    return p;
  }
  
  public RequestHandlers(SolrCore core) {
      this.core = core;
  }

  /**
   * @return the RequestHandler registered at the given name 
   */
  public SolrRequestHandler get(String handlerName) {
    return handlers.get(normalize(handlerName));
  }

  /**
   * @return a Map of all registered handlers of the specified type.
   */
  public <T extends SolrRequestHandler> Map<String,T> getAll(Class<T> clazz) {
    Map<String,T> result = new HashMap<>(7);
    for (Map.Entry<String,SolrRequestHandler> e : handlers.entrySet()) {
      if(clazz.isInstance(e.getValue())) result.put(e.getKey(), clazz.cast(e.getValue()));
    }
    return result;
  }

  /**
   * Handlers must be initialized before calling this function.  As soon as this is
   * called, the handler can immediately accept requests.
   * 
   * This call is thread safe.
   * 
   * @return the previous handler at the given path or null
   */
  public SolrRequestHandler register( String handlerName, SolrRequestHandler handler ) {
    String norm = normalize( handlerName );
    if (handler == null) {
      return handlers.remove(norm);
    }
    SolrRequestHandler old = handlers.put(norm, handler);
    if (0 != norm.length() && handler instanceof SolrInfoMBean) {
      core.getInfoRegistry().put(handlerName, handler);
    }
    return old;
  }

  /**
   * Returns an unmodifiable Map containing the registered handlers
   */
  public Map<String,SolrRequestHandler> getRequestHandlers() {
    return immutableHandlers;
  }


  /**
   * Read solrconfig.xml and register the appropriate handlers
   * 
   * This function should <b>only</b> be called from the SolrCore constructor.  It is
   * not intended as a public API.
   * 
   * While the normal runtime registration contract is that handlers MUST be initialized
   * before they are registered, this function does not do that exactly.
   *
   * This function registers all handlers first and then calls init() for each one.
   *
   * This is OK because this function is only called at startup and there is no chance that
   * a handler could be asked to handle a request before it is initialized.
   * 
   * The advantage to this approach is that handlers can know what path they are registered
   * to and what other handlers are available at startup.
   * 
   * Handlers will be registered and initialized in the order they appear in solrconfig.xml
   */

  void initHandlersFromConfig(SolrConfig config){
    List<PluginInfo> implicits = PluginsRegistry.getHandlers(core);
    // use link map so we iterate in the same order
    Map<PluginInfo,SolrRequestHandler> handlers = new LinkedHashMap<>();
    Map<String, PluginInfo> infoMap= new LinkedHashMap<>();
    //deduping implicit and explicit requesthandlers
    for (PluginInfo info : implicits) infoMap.put(info.name,info);
    for (PluginInfo info : config.getPluginInfos(SolrRequestHandler.class.getName()))
      if (infoMap.containsKey(info.name)) infoMap.remove(info.name);
    for (Map.Entry e : core.getSolrConfig().getOverlay().getReqHandlers().entrySet())
      infoMap.put((String)e.getKey(), new PluginInfo(SolrRequestHandler.TYPE, (Map)e.getValue()));

    ArrayList<PluginInfo> infos = new ArrayList<>(infoMap.values());
    infos.addAll(config.getPluginInfos(SolrRequestHandler.class.getName()));
    for (PluginInfo info : infos) {
      try {
        SolrRequestHandler requestHandler;
        String startup = info.attributes.get("startup");
        String lib = info.attributes.get("lib");
        if (lib != null) {
          requestHandler = new DynamicLazyRequestHandlerWrapper(core);
        } else if (startup != null) {
          if ("lazy".equals(startup)) {
            log.info("adding lazy requestHandler: " + info.className);
            requestHandler = new LazyRequestHandlerWrapper(core);
          } else {
            throw new Exception("Unknown startup value: '" + startup + "' for: " + info.className);
          }
        } else {
          requestHandler = core.createRequestHandler(info.className);
        }
        if (requestHandler instanceof RequestHandlerBase) ((RequestHandlerBase) requestHandler).setPluginInfo(info);
        
        handlers.put(info, requestHandler);
        SolrRequestHandler old = register(info.name, requestHandler);
        if (old != null) {
          log.warn("Multiple requestHandler registered to the same name: " + info.name + " ignoring: " + old.getClass().getName());
        }
        if (info.isDefault()) {
          old = register("", requestHandler);
          if (old != null) log.warn("Multiple default requestHandler registered" + " ignoring: " + old.getClass().getName());
        }
        log.info("created " + info.name + ": " + info.className);
      } catch (Exception ex) {
          throw new SolrException
            (ErrorCode.SERVER_ERROR, "RequestHandler init failure", ex);
      }
    }

    // we've now registered all handlers, time to init them in the same order
    for (Map.Entry<PluginInfo,SolrRequestHandler> entry : handlers.entrySet()) {
      PluginInfo info = entry.getKey();
      SolrRequestHandler requestHandler = entry.getValue();
      info = applyInitParams(config, info);
      if (requestHandler instanceof PluginInfoInitialized) {
       ((PluginInfoInitialized) requestHandler).init(info);
      } else{
        requestHandler.init(info.initArgs);
      }
    }

    if(get("") == null) register("", get("/select"));//defacto default handler
    if(get("") == null) register("", get("standard"));//old default handler name; TODO remove?
    if(get("") == null)
      log.warn("no default request handler is registered (either '/select' or 'standard')");
  }

  private PluginInfo applyInitParams(SolrConfig config, PluginInfo info) {
    List<InitParams> ags = new ArrayList<>();
    String p = info.attributes.get(InitParams.TYPE);
    if(p!=null) {
      for (String arg : StrUtils.splitSmart(p, ',')) {
        if(config.getInitParams().containsKey(arg)) ags.add(config.getInitParams().get(arg));
        else log.warn("INVALID paramSet {} in requestHandler {}", arg, info.toString());
      }
    }
    for (InitParams args : config.getInitParams().values())
      if(args.matchPath(info.name)) ags.add(args);
    if(!ags.isEmpty()){
      info = info.copy();
      for (InitParams initParam : ags) {
        initParam.apply(info);
      }
    }
    return info;
  }


  /**
   * The <code>LazyRequestHandlerWrapper</code> wraps any {@link SolrRequestHandler}.  
   * Rather then instantiate and initialize the handler on startup, this wrapper waits
   * until it is actually called.  This should only be used for handlers that are
   * unlikely to be used in the normal lifecycle.
   * 
   * You can enable lazy loading in solrconfig.xml using:
   * 
   * <pre>
   *  &lt;requestHandler name="..." class="..." startup="lazy"&gt;
   *    ...
   *  &lt;/requestHandler&gt;
   * </pre>
   * 
   * This is a private class - if there is a real need for it to be public, it could
   * move
   * 
   * @since solr 1.2
   */
  public static class LazyRequestHandlerWrapper implements SolrRequestHandler , AutoCloseable, PluginInfoInitialized
  {
    private final SolrCore core;
    String _className;
    SolrRequestHandler _handler;
    PluginInfo _pluginInfo;
    
    public LazyRequestHandlerWrapper( SolrCore core)
    {
      this.core = core;
      _handler = null; // don't initialize
    }

    @Override
    public void init(NamedList args) { }

    /**
     * Wait for the first request before initializing the wrapped handler 
     */
    @Override
    public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp)  {
      SolrRequestHandler handler = _handler;
      if (handler == null) {
        handler = getWrappedHandler();
      }
      handler.handleRequest( req, rsp );
    }

    public synchronized SolrRequestHandler getWrappedHandler()
    {
      if( _handler == null ) {
        try {
          SolrRequestHandler handler = createRequestHandler();
          if (handler instanceof PluginInfoInitialized) {
            ((PluginInfoInitialized) handler).init(_pluginInfo);
          } else {
            handler.init( _pluginInfo.initArgs );
          }

          if (handler instanceof PluginInfoInitialized) {
            ((PluginInfoInitialized) handler).init(_pluginInfo);
          } else {
            handler.init( _pluginInfo.initArgs );
          }


          if( handler instanceof SolrCoreAware ) {
            ((SolrCoreAware)handler).inform( core );
          }
          if (handler instanceof RequestHandlerBase) ((RequestHandlerBase) handler).setPluginInfo(_pluginInfo);
          _handler = handler;
        }
        catch( Exception ex ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "lazy loading error", ex );
        }
      }
      return _handler;
    }

    protected SolrRequestHandler createRequestHandler() {
      return core.createRequestHandler(_className);
    }

    public String getHandlerClass()
    {
      return _className;
    }
    
    //////////////////////// SolrInfoMBeans methods //////////////////////

    @Override
    public String getName() {
      return "Lazy["+_className+"]";
    }

    @Override
    public String getDescription()
    {
      if( _handler == null ) {
        return getName();
      }
      return _handler.getDescription();
    }
    
    @Override
    public String getVersion() {
      if( _handler != null ) {
        return _handler.getVersion();
      }
      return null;
    }

    @Override
    public String getSource() {
      return null;
    }
      
    @Override
    public URL[] getDocs() {
      if( _handler == null ) {
        return null;
      }
      return _handler.getDocs();
    }

    @Override
    public Category getCategory()
    {
      return Category.QUERYHANDLER;
    }

    @Override
    public NamedList getStatistics() {
      if( _handler != null ) {
        return _handler.getStatistics();
      }
      NamedList<String> lst = new SimpleOrderedMap<>();
      lst.add("note", "not initialized yet" );
      return lst;
    }

    @Override
    public void close() throws Exception {
      if (_handler == null) return;
      if (_handler instanceof AutoCloseable && !(_handler instanceof DynamicLazyRequestHandlerWrapper)) {
        ((AutoCloseable) _handler).close();
      }
    }

    @Override
    public void init(PluginInfo info) {
      _pluginInfo = info;
      _className = info.className;
    }
  }

  public static class DynamicLazyRequestHandlerWrapper extends LazyRequestHandlerWrapper {
    private String lib;
    private String key;
    private String version;
    private CoreContainer coreContainer;
    private SolrResourceLoader solrResourceLoader;
    private MemClassLoader classLoader;
    private boolean _closed = false;
    boolean unrecoverable = false;
    String errMsg = null;
    private Exception exception;


    public DynamicLazyRequestHandlerWrapper(SolrCore core) {
      super(core);
      this.coreContainer = core.getCoreDescriptor().getCoreContainer();
      this.solrResourceLoader = core.getResourceLoader();

    }

    @Override
    public void init(PluginInfo info) {
      super.init(info);
      this.lib = _pluginInfo.attributes.get("lib");

      if(disableExternalLib){
        errMsg = "ERROR external library loading is disabled";
        unrecoverable = true;
        _handler = this;
        log.error(errMsg);
        return;
      }

      version = _pluginInfo.attributes.get("version");
      if (version == null) {
        errMsg = "ERROR 'lib' attribute must be accompanied with version also";
        unrecoverable = true;
        _handler = this;
        log.error(errMsg);
        return;
      }
      classLoader = new MemClassLoader(this);
    }

    @Override
    public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
      if(unrecoverable) {
        rsp.add("error", errMsg);
        if(exception != null) rsp.setException(exception);
        return;
      }
      try {
        classLoader.checkJarAvailable();
      } catch (SolrException e) {
        rsp.add("error", "Jar could not be loaded");
        rsp.setException(e);
        return;
      } catch (IOException e) {
        unrecoverable = true;
        errMsg = "Could not load jar";
        exception = e;
        handleRequest(req,rsp);
        return;
      }

      super.handleRequest(req, rsp);
    }

    @Override
    protected SolrRequestHandler createRequestHandler() {
      try {
        Class clazz =  classLoader.findClass(_className);
        Constructor<?>[] cons =  clazz.getConstructors();
        for (Constructor<?> con : cons) {
          Class<?>[] types = con.getParameterTypes();
          if(types.length == 1 && types[0] == SolrCore.class){
            return SolrRequestHandler.class.cast(con.newInstance(this));
          }
        }
        return (SolrRequestHandler)clazz.newInstance();
      } catch (Exception e) {
        unrecoverable = true;
        errMsg = MessageFormat.format("class {0} could not be loaded ",_className);
        this.exception = e;
        return this;

      }

    }

    @Override
    public void close() throws Exception {
      super.close();
      if (_closed) return;
      classLoader.releaseJar();
      _closed = true;
    }
  }


  public static class MemClassLoader extends ClassLoader {
    private JarRepository.JarContentRef jarContent;
    private final DynamicLazyRequestHandlerWrapper handlerWrapper;
    public MemClassLoader(DynamicLazyRequestHandlerWrapper handlerWrapper) {
      super(handlerWrapper.solrResourceLoader.classLoader);
      this.handlerWrapper = handlerWrapper;

    }

    boolean checkJarAvailable() throws IOException {
      if (jarContent != null) return true;

      try {
        synchronized (this) {
          jarContent = handlerWrapper.coreContainer.getJarRepository().getJarIncRef(handlerWrapper.lib+"/"+handlerWrapper.version);
          return true;
        }
      } catch (SolrException se) {
        throw se;
      }

    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      try {
        return super.findClass(name);
      } catch (ClassNotFoundException e) {
        String path = name.replace('.', '/').concat(".class");
        ByteBuffer buf = null;
        try {
          if(jarContent == null) checkJarAvailable();
          buf = jarContent.jar.getFileContent(path);
          if(buf==null) throw new ClassNotFoundException("class not found in loaded jar"+ name ) ;
        } catch (IOException e1) {
          throw new ClassNotFoundException("class not found "+ name ,e1) ;

        }

        ProtectionDomain defaultDomain = null;

        //using the default protection domain, with no permissions
        try {
          defaultDomain = new ProtectionDomain(new CodeSource(new URL("http://localhost/.system/blob/"+handlerWrapper.lib), (Certificate[]) null),
              null);
        } catch (MalformedURLException e1) {
          //should not happen
        }
        return defineClass(name,buf.array(),buf.arrayOffset(),buf.limit(), defaultDomain);
      }
    }


    private void releaseJar(){
      handlerWrapper.coreContainer.getJarRepository().decrementJarRefCount(jarContent);
    }

  }

  public void close() {
    for (Map.Entry<String, SolrRequestHandler> e : handlers.entrySet()) {
      if (e.getValue() instanceof AutoCloseable) {
        try {
          ((AutoCloseable) e.getValue()).close();
        } catch (Exception exp) {
          log.error("Error closing requestHandler "+e.getKey() , exp);
        }
      }

    }

  }
}







