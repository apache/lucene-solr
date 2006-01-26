/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.solr.util.DOMUtil;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.StandardRequestHandler;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathConstants;
import java.util.logging.Logger;
import java.util.HashMap;

/**
 * @author yonik
 */
final class RequestHandlers {
  public static Logger log = Logger.getLogger(org.apache.solr.core.RequestHandlers.class.getName());

  public static final String DEFAULT_HANDLER_NAME="standard";

  final HashMap<String, SolrRequestHandler> map = new HashMap<String,SolrRequestHandler>();

  public RequestHandlers(Config config) {
    NodeList nodes = (NodeList)config.evaluate("requestHandler", XPathConstants.NODESET);

    if (nodes!=null) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);

        // We can tolerate an error in some request handlers, still load the
        // others, and have a working system.
        try {
          String name = DOMUtil.getAttr(node,"name","requestHandler config");
          String className = DOMUtil.getAttr(node,"class","requestHandler config");
          log.info("adding requestHandler " + name + "=" + className);

          SolrRequestHandler handler = (SolrRequestHandler) Config.newInstance(className);
          handler.init(DOMUtil.childNodesToNamedList(node));

          put(name, handler);

        } catch (Exception e) {
          SolrException.logOnce(log,null,e);
        }
      }
    }

    //
    // Get the default handler and add it in the map under null and empty
    // to act as the default.
    //
    SolrRequestHandler handler = get(DEFAULT_HANDLER_NAME);
    if (handler == null) {
      handler = new StandardRequestHandler();
      put(DEFAULT_HANDLER_NAME, handler);
    }
    put(null, handler);
    put("", handler);

  }

  public SolrRequestHandler get(String handlerName) {
    return map.get(handlerName);
  }

  public void put(String handlerName, SolrRequestHandler handler) {
    map.put(handlerName, handler);
    if (handlerName != null && handlerName != "") {
      if (handler instanceof SolrInfoMBean) {
        SolrInfoRegistry.getRegistry().put(handlerName, (SolrInfoMBean)handler);
      }
    }
  }

}
