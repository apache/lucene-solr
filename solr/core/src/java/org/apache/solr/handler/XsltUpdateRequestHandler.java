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

package org.apache.solr.handler;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;

/**
 * Add documents to solr using the STAX XML parser, transforming it with XSLT first
 */
public class XsltUpdateRequestHandler extends ContentStreamHandlerBase {
  public static Logger log = LoggerFactory.getLogger(XsltUpdateRequestHandler.class);
  public static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  public static final int XSLT_CACHE_DEFAULT = 60;
  private static final String XSLT_CACHE_PARAM = "xsltCacheLifetimeSeconds"; 

  XMLInputFactory inputFactory;
  private Integer xsltCacheLifetimeSeconds;

  @Override
  public void init(NamedList args) {
    super.init(args);

    inputFactory = XMLInputFactory.newInstance();
    try {
      // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
      // XMLInputFactory, as that implementation tries to cache and reuse the
      // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
      // prevents this.
      // All other known open-source stax parsers (and the bea ref impl)
      // have thread-safe factories.
      inputFactory.setProperty("reuse-instance", Boolean.FALSE);
    }
    catch (IllegalArgumentException ex) {
      // Other implementations will likely throw this exception since "reuse-instance"
      // isimplementation specific.
      log.debug("Unable to set the 'reuse-instance' property for the input chain: " + inputFactory);
    }
    inputFactory.setXMLReporter(xmllog);
    
    final SolrParams p = SolrParams.toSolrParams(args);
    this.xsltCacheLifetimeSeconds = p.getInt(XSLT_CACHE_PARAM,XSLT_CACHE_DEFAULT);
    log.info("xsltCacheLifetimeSeconds=" + xsltCacheLifetimeSeconds);
  }

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new XsltXMLLoader(processor, inputFactory, xsltCacheLifetimeSeconds);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Add documents with XML, transforming with XSLT first";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }


}
