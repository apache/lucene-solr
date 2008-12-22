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
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;

/**
 * Add documents to solr using the STAX XML parser.
 */
public class XmlUpdateRequestHandler extends ContentStreamHandlerBase {
  public static Logger log = LoggerFactory.getLogger(XmlUpdateRequestHandler.class);

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
  public static final String COMMIT_WITHIN = "commitWithin";
  public static final String OVERWRITE_COMMITTED = "overwriteCommitted"; // @Deprecated
  public static final String OVERWRITE_PENDING = "overwritePending";  // @Deprecated
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
  }

  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new XMLLoader(processor, inputFactory);
  }


  /**
   * A Convenience method for getting back a simple XML string indicating
   * success or failure from an XML formated Update (from the Reader)
   *
   * @since solr 1.2
   * @deprecated Use {@link XMLLoader#load(SolrQueryRequest,SolrQueryResponse,ContentStream)} instead.
   */
  @Deprecated
  public void doLegacyUpdate(Reader input, Writer output) {
    try {
      SolrCore core = SolrCore.getSolrCore();

      // Old style requests do not choose a custom handler
      UpdateRequestProcessorChain processorFactory = core.getUpdateProcessingChain(null);

      SolrParams params = new MapSolrParams(new HashMap<String, String>());
      SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {
      };
      SolrQueryResponse rsp = new SolrQueryResponse(); // ignored
      XMLStreamReader parser = inputFactory.createXMLStreamReader(input);
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



