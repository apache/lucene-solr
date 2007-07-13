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

package org.apache.solr.update.processor;

import java.util.ArrayList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * An UpdateRequestProcessorFactory that constructs a chain of UpdateRequestProcessor.
 * 
 * This is the default implementation and can be configured via solrconfig.xml with:
 * 
 * <updateRequestProcessor>
 *   <factory name="standard" class="solr.ChainedUpdateProcessorFactory" >
 *     <chain class="PathToClass1" />
 *     <chain class="PathToClass2" />
 *     <chain class="solr.LogUpdateProcessorFactory" >
 *      <int name="maxNumToLog">100</int>
 *     </chain>
 *     <chain class="solr.RunUpdateProcessorFactory" />
 *   </factory>
 * </updateRequestProcessor>
 * 
 * @since solr 1.3
 */
public class ChainedUpdateProcessorFactory extends UpdateRequestProcessorFactory 
{
  UpdateRequestProcessorFactory[] factory;
  
  @Override
  public void init( Node node ) {
    final ArrayList<UpdateRequestProcessorFactory> factories = new ArrayList<UpdateRequestProcessorFactory>();
    if( node != null ) {
      // Load and initialize the plugin chain
      AbstractPluginLoader<UpdateRequestProcessorFactory> loader 
          = new AbstractPluginLoader<UpdateRequestProcessorFactory>( "processor chain", false, false ) {
        @Override
        protected void init(UpdateRequestProcessorFactory plugin, Node node) throws Exception {
          plugin.init( node );
        }
  
        @Override
        protected UpdateRequestProcessorFactory register(String name, UpdateRequestProcessorFactory plugin) throws Exception {
          factories.add( plugin );
          return null;
        }
      };
      
      XPath xpath = XPathFactory.newInstance().newXPath();
      try {
        loader.load( (NodeList) xpath.evaluate( "chain", node, XPathConstants.NODESET ) );
      } 
      catch (XPathExpressionException e) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
            "Error loading processor chain: " + node,e,false);
      }
    }
    
    // If not configured, make sure it has the default settings
    if( factories.size() < 1 ) {
      factories.add( new RunUpdateProcessorFactory() );
      factories.add( new LogUpdateProcessorFactory() );
    }
    factory = factories.toArray( new UpdateRequestProcessorFactory[factories.size()] );
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) 
  {
    UpdateRequestProcessor processor = null;
    for (int i = factory.length-1; i>=0; i--) {
      processor = factory[i].getInstance(req, rsp, processor);
    }
    return processor;
  }
}
