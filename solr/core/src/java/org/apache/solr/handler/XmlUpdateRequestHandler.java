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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.Writer;
import java.util.HashMap;

/**
 * Add documents to solr using the STAX XML parser.
 */
public class XmlUpdateRequestHandler extends ContentStreamHandlerBase {
  public static Logger log = LoggerFactory.getLogger(XmlUpdateRequestHandler.class);
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  public static final String UPDATE_PROCESSOR = "update.processor";

  // XML Constants
  public static final String ADD = "add";
  public static final String DELETE = "delete";
  public static final String OPTIMIZE = "optimize";
  public static final String COMMIT = "commit";
  public static final String ROLLBACK = "rollback";
  public static final String WAIT_SEARCHER = "waitSearcher";
  public static final String WAIT_FLUSH = "waitFlush";

  public static final String OVERWRITE = "overwrite";

  //NOTE: This constant is for use with the <add> XML tag, not the HTTP param with same name
  public static final String COMMIT_WITHIN = "commitWithin";
  
  public static final String FROM_COMMITTED = "fromCommitted";
  public static final String FROM_PENDING = "fromPending";
  
  /**
   * @deprecated use {@link #OVERWRITE}
   */
  @Deprecated
  public static final String OVERWRITE_COMMITTED = "overwriteCommitted";
  
  /**
   * @deprecated use {@link #OVERWRITE}
   */
  @Deprecated
  public static final String OVERWRITE_PENDING = "overwritePending";

  /**
   * @deprecated use {@link #OVERWRITE}
   */
  @Deprecated
  public static final String ALLOW_DUPS = "allowDups";

  XMLInputFactory inputFactory;


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
  }

  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new XMLLoader(processor, inputFactory);
  }


  /**
   * A Convenience method for getting back a simple XML string indicating
   * success or failure from an XML formated Update (from the Reader)
   *
   * @since solr 1.2
   * @deprecated Direct updates from a Servlet, as well as the response 
   *             format produced by this method, have been deprecated 
   *             and will be removed in future versions.  Any code using
   *             this method should be changed to use {@link #handleRequest} 
   *             method with a ContentStream. 
   */
  @Deprecated
  public void doLegacyUpdate(InputStream input, String inputContentType, Writer output) {
    SolrCore core = SolrCore.getSolrCore();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new HashMap<String,String[]>());
    try {
      // Old style requests do not choose a custom handler
      UpdateRequestProcessorChain processorFactory = core.getUpdateProcessingChain(null);
      SolrQueryResponse rsp = new SolrQueryResponse(); // ignored
      final String charset = ContentStreamBase.getCharsetFromContentType(inputContentType);
      final XMLStreamReader parser = (charset == null) ?
        inputFactory.createXMLStreamReader(input) : inputFactory.createXMLStreamReader(input, charset);
      UpdateRequestProcessor processor = processorFactory.createProcessor(req, rsp);
      XMLLoader loader = (XMLLoader) newLoader(req, processor);
      loader.processUpdate(processor, parser);
      processor.finish();
      output.write("<result status=\"0\"></result>");
    }
    catch (Exception ex) {
      try {
        SolrException.logOnce(log, "Error processing \"legacy\" update command", ex);
        XML.writeXML(output, "result", SolrException.toStr(ex), "status", "1");
      } catch (Exception ee) {
        log.error("Error writing to output stream: " + ee);
      }
    }
    finally {
      req.close();
    }
  }
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Add documents with XML";
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



