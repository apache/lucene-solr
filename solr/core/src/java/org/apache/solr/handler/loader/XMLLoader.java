package org.apache.solr.handler.loader;
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

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.util.xslt.TransformerProvider;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.util.EmptyEntityResolver;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.parsers.SAXParserFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class XMLLoader extends ContentStreamLoader {
  public static Logger log = LoggerFactory.getLogger(XMLLoader.class);
  static final XMLErrorLogger xmllog = new XMLErrorLogger(log);
  
  public static final String CONTEXT_TRANSFORMER_KEY = "xsltupdater.transformer";

  private static final String XSLT_CACHE_PARAM = "xsltCacheLifetimeSeconds"; 

  public static final int XSLT_CACHE_DEFAULT = 60;
  
  int xsltCacheLifetimeSeconds;
  XMLInputFactory inputFactory;
  SAXParserFactory saxFactory;

  @Override
  public XMLLoader init(SolrParams args) {
    // Init StAX parser:
    inputFactory = XMLInputFactory.newInstance();
    EmptyEntityResolver.configureXMLInputFactory(inputFactory);
    inputFactory.setXMLReporter(xmllog);
    try {
      // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
      // XMLInputFactory, as that implementation tries to cache and reuse the
      // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
      // prevents this.
      // All other known open-source stax parsers (and the bea ref impl)
      // have thread-safe factories.
      inputFactory.setProperty("reuse-instance", Boolean.FALSE);
    } catch (IllegalArgumentException ex) {
      // Other implementations will likely throw this exception since "reuse-instance"
      // isimplementation specific.
      log.debug("Unable to set the 'reuse-instance' property for the input chain: " + inputFactory);
    }
    
    // Init SAX parser (for XSL):
    saxFactory = SAXParserFactory.newInstance();
    saxFactory.setNamespaceAware(true); // XSL needs this!
    EmptyEntityResolver.configureSAXParserFactory(saxFactory);
    
    xsltCacheLifetimeSeconds = XSLT_CACHE_DEFAULT;
    if(args != null) {
      xsltCacheLifetimeSeconds = args.getInt(XSLT_CACHE_PARAM,XSLT_CACHE_DEFAULT);
      log.info("xsltCacheLifetimeSeconds=" + xsltCacheLifetimeSeconds);
    }
    return this;
  }

  public String getDefaultWT() {
    return "xml";
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    final String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
    
    InputStream is = null;
    XMLStreamReader parser = null;

    String tr = req.getParams().get(CommonParams.TR,null);
    if(tr!=null) {
      final Transformer t = getTransformer(tr,req);
      final DOMResult result = new DOMResult();
      
      // first step: read XML and build DOM using Transformer (this is no overhead, as XSL always produces
      // an internal result DOM tree, we just access it directly as input for StAX):
      try {
        is = stream.getStream();
        final InputSource isrc = new InputSource(is);
        isrc.setEncoding(charset);
        final XMLReader xmlr = saxFactory.newSAXParser().getXMLReader();
        xmlr.setErrorHandler(xmllog);
        xmlr.setEntityResolver(EmptyEntityResolver.SAX_INSTANCE);
        final SAXSource source = new SAXSource(xmlr, isrc);
        t.transform(source, result);
      } catch(TransformerException te) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, te.getMessage(), te);
      } finally {
        IOUtils.closeQuietly(is);
      }
      // second step: feed the intermediate DOM tree into StAX parser:
      try {
        parser = inputFactory.createXMLStreamReader(new DOMSource(result.getNode()));
        this.processUpdate(req, processor, parser);
      } catch (XMLStreamException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
      } finally {
        if (parser != null) parser.close();
      }
    }
    // Normal XML Loader
    else {
      try {
        is = stream.getStream();
        if (UpdateRequestHandler.log.isTraceEnabled()) {
          final byte[] body = IOUtils.toByteArray(is);
          // TODO: The charset may be wrong, as the real charset is later
          // determined by the XML parser, the content-type is only used as a hint!
          UpdateRequestHandler.log.trace("body", new String(body, (charset == null) ?
            ContentStreamBase.DEFAULT_CHARSET : charset));
          IOUtils.closeQuietly(is);
          is = new ByteArrayInputStream(body);
        }
        parser = (charset == null) ?
          inputFactory.createXMLStreamReader(is) : inputFactory.createXMLStreamReader(is, charset);
        this.processUpdate(req, processor, parser);
      } catch (XMLStreamException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
      } finally {
        if (parser != null) parser.close();
        IOUtils.closeQuietly(is);
      }
    }
  }
  

  /** Get Transformer from request context, or from TransformerProvider.
   *  This allows either getContentType(...) or write(...) to instantiate the Transformer,
   *  depending on which one is called first, then the other one reuses the same Transformer
   */
  Transformer getTransformer(String xslt, SolrQueryRequest request) throws IOException {
    // not the cleanest way to achieve this
    // no need to synchronize access to context, right? 
    // Nothing else happens with it at the same time
    final Map<Object,Object> ctx = request.getContext();
    Transformer result = (Transformer)ctx.get(CONTEXT_TRANSFORMER_KEY);
    if(result==null) {
      SolrConfig solrConfig = request.getCore().getSolrConfig();
      result = TransformerProvider.instance.getTransformer(solrConfig, xslt, xsltCacheLifetimeSeconds);
      result.setErrorListener(xmllog);
      ctx.put(CONTEXT_TRANSFORMER_KEY,result);
    }
    return result;
  }


  /**
   * @since solr 1.2
   */
  void processUpdate(SolrQueryRequest req, UpdateRequestProcessor processor, XMLStreamReader parser)
          throws XMLStreamException, IOException, FactoryConfigurationError {
    AddUpdateCommand addCmd = null;
    SolrParams params = req.getParams();
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.END_DOCUMENT:
          parser.close();
          return;

        case XMLStreamConstants.START_ELEMENT:
          String currTag = parser.getLocalName();
          if (currTag.equals(UpdateRequestHandler.ADD)) {
            log.trace("SolrCore.update(add)");

            addCmd = new AddUpdateCommand(req);

            // First look for commitWithin parameter on the request, will be overwritten for individual <add>'s
            addCmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
            addCmd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
            
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (UpdateRequestHandler.OVERWRITE.equals(attrName)) {
                addCmd.overwrite = StrUtils.parseBoolean(attrVal);
              } else if (UpdateRequestHandler.COMMIT_WITHIN.equals(attrName)) {
                addCmd.commitWithin = Integer.parseInt(attrVal);
              } else {
                log.warn("Unknown attribute id in add:" + attrName);
              }
            }

          } else if ("doc".equals(currTag)) {
            if(addCmd != null) {
              log.trace("adding doc...");
              addCmd.clear();
              addCmd.solrDoc = readDoc(parser);
              processor.processAdd(addCmd);
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unexpected <doc> tag without an <add> tag surrounding it.");
            }
          } else if (UpdateRequestHandler.COMMIT.equals(currTag) || UpdateRequestHandler.OPTIMIZE.equals(currTag)) {
            log.trace("parsing " + currTag);

            CommitUpdateCommand cmd = new CommitUpdateCommand(req, UpdateRequestHandler.OPTIMIZE.equals(currTag));
            ModifiableSolrParams mp = new ModifiableSolrParams();
            
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              mp.set(attrName, attrVal);
            }

            RequestHandlerUtils.validateCommitParams(mp);
            SolrParams p = SolrParams.wrapDefaults(mp, req.getParams());   // default to the normal request params for commit options
            RequestHandlerUtils.updateCommit(cmd, p);

            processor.processCommit(cmd);
          } // end commit
          else if (UpdateRequestHandler.ROLLBACK.equals(currTag)) {
            log.trace("parsing " + currTag);

            RollbackUpdateCommand cmd = new RollbackUpdateCommand(req);

            processor.processRollback(cmd);
          } // end rollback
          else if (UpdateRequestHandler.DELETE.equals(currTag)) {
            log.trace("parsing delete");
            processDelete(req, processor, parser);
          } // end delete
          break;
      }
    }
  }

  /**
   * @since solr 1.3
   */
  void processDelete(SolrQueryRequest req, UpdateRequestProcessor processor, XMLStreamReader parser) throws XMLStreamException, IOException {
    // Parse the command
    DeleteUpdateCommand deleteCmd = new DeleteUpdateCommand(req);

    // First look for commitWithin parameter on the request, will be overwritten for individual <delete>'s
    SolrParams params = req.getParams();
    deleteCmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);

    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String attrName = parser.getAttributeLocalName(i);
      String attrVal = parser.getAttributeValue(i);
      if ("fromPending".equals(attrName)) {
        // deprecated
      } else if ("fromCommitted".equals(attrName)) {
        // deprecated
      } else if (UpdateRequestHandler.COMMIT_WITHIN.equals(attrName)) {
        deleteCmd.commitWithin = Integer.parseInt(attrVal);
      } else {
        log.warn("unexpected attribute delete/@" + attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          String mode = parser.getLocalName();
          if (!("id".equals(mode) || "query".equals(mode))) {
            log.warn("unexpected XML tag /delete/" + mode);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag /delete/" + mode);
          }
          text.setLength(0);
          
          if ("id".equals(mode)) {
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (UpdateRequestHandler.VERSION.equals(attrName)) {
                deleteCmd.setVersion(Long.parseLong(attrVal));
              }
            }
          }
          break;

        case XMLStreamConstants.END_ELEMENT:
          String currTag = parser.getLocalName();
          if ("id".equals(currTag)) {
            deleteCmd.setId(text.toString());         
          } else if ("query".equals(currTag)) {
            deleteCmd.setQuery(text.toString());
          } else if ("delete".equals(currTag)) {
            return;
          } else {
            log.warn("unexpected XML tag /delete/" + currTag);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag /delete/" + currTag);
          }
          processor.processDelete(deleteCmd);
          deleteCmd.clear();
          break;

          // Add everything to the text
        case XMLStreamConstants.SPACE:
        case XMLStreamConstants.CDATA:
        case XMLStreamConstants.CHARACTERS:
          text.append(parser.getText());
          break;
      }
    }
  }


  /**
   * Given the input stream, read a document
   *
   * @since solr 1.3
   */
  public SolrInputDocument readDoc(XMLStreamReader parser) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();

    String attrName = "";
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      attrName = parser.getAttributeLocalName(i);
      if ("boost".equals(attrName)) {
        doc.setDocumentBoost(Float.parseFloat(parser.getAttributeValue(i)));
      } else {
        log.warn("Unknown attribute doc/@" + attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    String name = null;
    float boost = 1.0f;
    boolean isNull = false;
    String update = null;

    while (true) {
      int event = parser.next();
      switch (event) {
        // Add everything to the text
        case XMLStreamConstants.SPACE:
        case XMLStreamConstants.CDATA:
        case XMLStreamConstants.CHARACTERS:
          text.append(parser.getText());
          break;

        case XMLStreamConstants.END_ELEMENT:
          if ("doc".equals(parser.getLocalName())) {
            return doc;
          } else if ("field".equals(parser.getLocalName())) {
            Object v = isNull ? null : text.toString();
            if (update != null) {
              Map<String,Object> extendedValue = new HashMap<String,Object>(1);
              extendedValue.put(update, v);
              v = extendedValue;
            }
            doc.addField(name, v, boost);
            boost = 1.0f;
          }
          break;

        case XMLStreamConstants.START_ELEMENT:
          text.setLength(0);
          String localName = parser.getLocalName();
          if (!"field".equals(localName)) {
            log.warn("unexpected XML tag doc/" + localName);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag doc/" + localName);
          }
          boost = 1.0f;
          update = null;
          String attrVal = "";
          for (int i = 0; i < parser.getAttributeCount(); i++) {
            attrName = parser.getAttributeLocalName(i);
            attrVal = parser.getAttributeValue(i);
            if ("name".equals(attrName)) {
              name = attrVal;
            } else if ("boost".equals(attrName)) {
              boost = Float.parseFloat(attrVal);
            } else if ("null".equals(attrName)) {
              isNull = StrUtils.parseBoolean(attrVal);
            } else if ("update".equals(attrName)) {
              update = attrVal;
            } else {
              log.warn("Unknown attribute doc/field/@" + attrName);
            }
          }
          break;
      }
    }
  }
}
