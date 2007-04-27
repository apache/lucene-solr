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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.xpath.XPathConstants;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.StandardRequestHandler;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SimpleOrderedMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author yonik
 */
final class RequestHandlers {
  public static Logger log = Logger.getLogger(RequestHandlers.class.getName());

  public static final String DEFAULT_HANDLER_NAME="standard";

  // Use a synchronized map - since the handlers can be changed at runtime, 
  // the map implementaion should be thread safe
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
    if( p != null && p.endsWith( "/" ) )
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
   * Handlers must be initalized before calling this function.  As soon as this is
   * called, the handler can immediatly accept requests.
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
   * Returns an unmodifieable Map containing the registered handlers
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
   * While the normal runtime registration contract is that handlers MUST be initalizad 
   * before they are registered, this function does not do that exactly.
   * 
   * This funciton registers all handlers first and then calls init() for each one.  
   * 
   * This is OK because this function is only called at startup and there is no chance that
   * a handler could be asked to handle a request before it is initalized.
   * 
   * The advantage to this approach is that handlers can know what path they are registered
   * to and what other handlers are avaliable at startup.
   * 
   * Handlers will be registered and initalized in the order they appear in solrconfig.xml
   */
  @SuppressWarnings("unchecked")
  void initHandlersFromConfig( Config config )  
  {
    NodeList nodes = (NodeList)config.evaluate("requestHandler", XPathConstants.NODESET);
    
    if (nodes !=null ) {
      // make sure it only once/handler and that that handlers get initalized in the 
      // order they were defined
      Map<String,NamedList<Object>> names = new LinkedHashMap<String,NamedList<Object>>(); 
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
  
        // In a production environment, we can tolerate an error in some request handlers, 
        // still load the others, and have a working system.
        try {
          String name = DOMUtil.getAttr(node,"name","requestHandler config");
          String className = DOMUtil.getAttr(node,"class","requestHandler config");
          String startup = DOMUtil.getAttr(node,"startup", null );
          NamedList<Object> args = DOMUtil.childNodesToNamedList(node);
  
          // Perhaps lazy load the request handler with a wrapper
          SolrRequestHandler handler = null;
          if( "lazy".equals( startup ) ) {
            log.info("adding lazy requestHandler: " + name + "=" + className);
            handler = new LazyRequestHandlerWrapper( className, args );
          }
          else {
            Class<? extends SolrRequestHandler> clazz = Config.findClass( className, new String[]{} );
            log.info("adding requestHandler: " + name + "=" + className);
            handler = clazz.newInstance();
          }
          
          SolrRequestHandler old = register( name, handler );
          if( old != null ) {
            // TODO: SOLR-179?
            log.warning( "multiple handlers registered on the same path! ignoring: "+old );
          }
          names.put( name, args );
        } 
        catch (Exception e) {
          SolrConfig.severeErrors.add( e );
          SolrException.logOnce(log,null,e);
        }
      }
      
      // Call init() on each handler after they have all been registered
      for( Map.Entry<String, NamedList<Object>> reg : names.entrySet() ) {
        try {
          handlers.get( reg.getKey() ).init( reg.getValue() );
        }
        catch( Exception e ) {
          SolrConfig.severeErrors.add( e );
          SolrException.logOnce(log,null,e);
        }
      }
    }
    
    //
    // Get the default handler and add it in the map under null and empty
    // to act as the default.
    //
    SolrRequestHandler handler = get(RequestHandlers.DEFAULT_HANDLER_NAME);
    if (handler == null) {
      handler = new StandardRequestHandler();
      register(RequestHandlers.DEFAULT_HANDLER_NAME, handler);
    }
    register(null, handler);
    register("", handler);
  }
    

  /**
   * The <code>LazyRequestHandlerWrapper</core> wraps any {@link SolrRequestHandler}.  
   * Rather then instanciate and initalize the handler on startup, this wrapper waits
   * untill it is actually called.  This should only be used for handlers that are
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
   * @author ryan
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
      _handler = null; // don't initalize
    }
    
    /**
     * In normal use, this function will not be called
     */
    public void init(NamedList args) {
      // do nothing
    }
    
    /**
     * Wait for the first request before initalizing the wrapped handler 
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
          throw new SolrException( 500, "lazy loading error", ex );
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
      lst.add("note", "not initaized yet" );
      return lst;
    }
  }
}




