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

package org.apache.solr.core;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.xpath.XPathConstants;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.StandardRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

/**
 */
final class RequestHandlers {
  public static Logger log = Logger.getLogger(RequestHandlers.class.getName());

  public static final String DEFAULT_HANDLER_NAME="standard";

  // Use a synchronized map - since the handlers can be changed at runtime, 
  // the map implementation should be thread safe
  private final Map<String, SolrRequestHandler> handlers = Collections.synchronizedMap(
      new HashMap<String,SolrRequestHandler>() );

  /**
   * Trim the trailing '/' if its there.
   * 
   * we want:
   *  /update/csv
   *  /update/csv/
   * to map to the same handler 
   * 
   */
  private static String normalize( String p )
  {
    if( p != null && p.endsWith( "/" ) && p.length() > 1 )
      return p.substring( 0, p.length()-1 );
    
    return p;
  }
  
  /**
   * @return the RequestHandler registered at the given name 
   */
  public SolrRequestHandler get(String handlerName) {
    return handlers.get(normalize(handlerName));
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
    if( handler == null ) {
      return handlers.remove( norm );
    }
    SolrRequestHandler old = handlers.put(norm, handler);
    if (handlerName != null && handlerName != "") {
      if (handler instanceof SolrInfoMBean) {
        SolrInfoRegistry.getRegistry().put(handlerName, (SolrInfoMBean)handler);
      }
    }
    return old;
  }

  /**
   * Returns an unmodifiable Map containing the registered handlers
   */
  public Map<String,SolrRequestHandler> getRequestHandlers() {
    return Collections.unmodifiableMap( handlers );
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
  void initHandlersFromConfig( Config config )  
  {
    final RequestHandlers handlers = this;
    AbstractPluginLoader<SolrRequestHandler> loader = 
      new AbstractPluginLoader<SolrRequestHandler>( "[solrconfig.xml] requestHandler", true )
    {
      @Override
      protected SolrRequestHandler create( String name, String className, Node node ) throws Exception
      {    
        String startup = DOMUtil.getAttr( node, "startup" );
        if( startup != null ) {
          if( "lazy".equals( startup ) ) {
            log.info("adding lazy requestHandler: " + className );
            NamedList args = DOMUtil.childNodesToNamedList(node);
            return new LazyRequestHandlerWrapper( className, args );
          }
          else {
            throw new Exception( "Unknown startup value: '"+startup+"' for: "+className );
          }
        }
        return super.create( name, className, node );
      }

      @Override
      protected SolrRequestHandler register(String name, SolrRequestHandler plugin) throws Exception {
        return handlers.register( name, plugin );
      }
      
      @Override
      protected void init(SolrRequestHandler plugin, Node node ) throws Exception {
        plugin.init( DOMUtil.childNodesToNamedList(node) );
      }      
    };
    
    NodeList nodes = (NodeList)config.evaluate("requestHandler", XPathConstants.NODESET);
    
    // Load the handlers and get the default one
    SolrRequestHandler defaultHandler = loader.load( nodes );
    if( defaultHandler == null ) {
      defaultHandler = get(RequestHandlers.DEFAULT_HANDLER_NAME);
      if( defaultHandler == null ) {
        defaultHandler = new StandardRequestHandler();
        register(RequestHandlers.DEFAULT_HANDLER_NAME, defaultHandler);
      }
    }
    register(null, defaultHandler);
    register("", defaultHandler);
  }
    

  /**
   * The <code>LazyRequestHandlerWrapper</core> wraps any {@link SolrRequestHandler}.  
   * Rather then instanciate and initalize the handler on startup, this wrapper waits
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
   * @version $Id$
   * @since solr 1.2
   */
  private static final class LazyRequestHandlerWrapper implements SolrRequestHandler, SolrInfoMBean
  {
    private String _className;
    private NamedList _args;
    private SolrRequestHandler _handler;
    
    public LazyRequestHandlerWrapper( String className, NamedList args )
    {
      _className = className;
      _args = args;
      _handler = null; // don't initialize
    }
    
    /**
     * In normal use, this function will not be called
     */
    public void init(NamedList args) {
      // do nothing
    }
    
    /**
     * Wait for the first request before initializing the wrapped handler 
     */
    public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp)  {
      getWrappedHandler().handleRequest( req, rsp );
    }

    public synchronized SolrRequestHandler getWrappedHandler() 
    {
      if( _handler == null ) {
        try {
          Class clazz = Config.findClass( _className, new String[]{} );
          _handler = (SolrRequestHandler)clazz.newInstance();
          _handler.init( _args );
        }
        catch( Exception ex ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "lazy loading error", ex );
        }
      }
      return _handler; 
    }

    public String getHandlerClass()
    {
      return _className;
    }
    
    //////////////////////// SolrInfoMBeans methods //////////////////////

    public String getName() {
      return "Lazy["+_className+"]";
    }

    public String getDescription()
    {
      if( _handler == null ) {
        return getName();
      }
      return _handler.getDescription();
    }
    
    public String getVersion() {
        String rev = "$Revision$";
        if( _handler != null ) {
          rev += " :: " + _handler.getVersion();
        }
        return rev;
    }

    public String getSourceId() {
      String rev = "$Id$";
      if( _handler != null ) {
        rev += " :: " + _handler.getSourceId();
      }
      return rev;
    }

    public String getSource() {
      String rev = "$URL$";
      if( _handler != null ) {
        rev += "\n" + _handler.getSource();
      }
      return rev;
    }
      
    public URL[] getDocs() {
      if( _handler == null ) {
        return null;
      }
      return _handler.getDocs();
    }

    public Category getCategory()
    {
      return Category.QUERYHANDLER;
    }

    public NamedList getStatistics() {
      if( _handler != null ) {
        return _handler.getStatistics();
      }
      NamedList<String> lst = new SimpleOrderedMap<String>();
      lst.add("note", "not initialized yet" );
      return lst;
    }
  }
}







