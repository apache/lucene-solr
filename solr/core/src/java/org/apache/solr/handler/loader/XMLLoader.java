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
package org.apache.solr.handler.loader;

import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.EmptyEntityResolver;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLLoader extends ContentStreamLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();
  protected static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  protected XMLInputFactory inputFactory;
  protected SAXParserFactory saxFactory;

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
      log.debug("Unable to set the 'reuse-instance' property for the input chain: {}", inputFactory);
    }
    
    // Init SAX parser (for XSL):
    saxFactory = SAXParserFactory.newInstance();
    saxFactory.setNamespaceAware(true); // XSL needs this!
    EmptyEntityResolver.configureSAXParserFactory(saxFactory);

    return this;
  }

  @Override
  public String getDefaultWT() {
    return "xml";
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    final String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
    
    InputStream is = null;
    XMLStreamReader parser = null;

    // Normal XML Loader
    try {
      is = stream.getStream();
      if (log.isTraceEnabled()) {
        final byte[] body = IOUtils.toByteArray(is);
        // TODO: The charset may be wrong, as the real charset is later
        // determined by the XML parser, the content-type is only used as a hint!
        if (log.isTraceEnabled()) {
          log.trace("body: {}", new String(body, (charset == null) ?
              ContentStreamBase.DEFAULT_CHARSET : charset));
        }
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

  /**
   * @since solr 1.2
   */
  protected void processUpdate(SolrQueryRequest req, UpdateRequestProcessor processor, XMLStreamReader parser)
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
                log.warn("XML element <add> has invalid XML attr: {}", attrName);
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
            log.trace("parsing {}", currTag);

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
            log.trace("parsing rollback");

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
        log.warn("XML element <delete> has invalid XML attr: {}", attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          String mode = parser.getLocalName();
          if (!(ID.equals(mode) || "query".equals(mode))) {
            String msg = "XML element <delete> has invalid XML child element: " + mode;
            log.warn(msg);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    msg);
          }
          text.setLength(0);
          
          if (ID.equals(mode)) {
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (UpdateRequestHandler.VERSION.equals(attrName)) {
                deleteCmd.setVersion(Long.parseLong(attrVal));
              }
              if (ShardParams._ROUTE_.equals(attrName)) {
                deleteCmd.setRoute(attrVal);
              }
            }
          }
          break;

        case XMLStreamConstants.END_ELEMENT:
          String currTag = parser.getLocalName();
          if (ID.equals(currTag)) {
            deleteCmd.setId(text.toString());         
          } else if ("query".equals(currTag)) {
            deleteCmd.setQuery(text.toString());
          } else if ("delete".equals(currTag)) {
            return;
          } else {
            String msg = "XML element <delete> has invalid XML (closing) child element: " + currTag;
            log.warn(msg);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    msg);
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
  @SuppressWarnings({"unchecked"})
  public SolrInputDocument readDoc(XMLStreamReader parser) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();

    String attrName = "";
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      attrName = parser.getAttributeLocalName(i);
      if ("boost".equals(attrName)) {
        String message = "Ignoring document boost: " + parser.getAttributeValue(i) + " as index-time boosts are not supported anymore";
        if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
          log.warn(message);
        } else {
          log.debug(message);
        }
      } else {
        log.warn("XML element <doc> has invalid XML attr: {}", attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    String name = null;
    boolean isNull = false;
    boolean isLabeledChildDoc = false;
    String update = null;
    Collection<SolrInputDocument> subDocs = null;
    Map<String, Map<String, Object>> updateMap = null;
    boolean complete = false;
    while (!complete) {
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
            if (subDocs != null && !subDocs.isEmpty()) {
              doc.addChildDocuments(subDocs);
              subDocs = null;
            }
            complete = true;
            break;
          } else if ("field".equals(parser.getLocalName())) {
            // should I warn in some text has been found too
            Object v = isNull ? null : text.toString();
            if (update != null) {
              if (updateMap == null) updateMap = new HashMap<>();
              Map<String, Object> extendedValues = updateMap.get(name);
              if (extendedValues == null) {
                extendedValues = new HashMap<>(1);
                updateMap.put(name, extendedValues);
              }
              Object val = extendedValues.get(update);
              if (val == null) {
                extendedValues.put(update, v);
              } else {
                // multiple val are present
                if (val instanceof List) {
                  @SuppressWarnings({"rawtypes"})
                  List list = (List) val;
                  list.add(v);
                } else {
                  List<Object> values = new ArrayList<>();
                  values.add(val);
                  values.add(v);
                  extendedValues.put(update, values);
                }
              }
              break;
            }
            if(!isLabeledChildDoc){
              // only add data if this is not a childDoc, since it was added already
              doc.addField(name, v);
            } else {
              // reset so next field is not treated as child doc
              isLabeledChildDoc = false;
            }
            // field is over
            name = null;
          }
          break;

        case XMLStreamConstants.START_ELEMENT:
          text.setLength(0);
          String localName = parser.getLocalName();
          if ("doc".equals(localName)) {
            if(name != null) {
              // flag to prevent spaces after doc from being added
              isLabeledChildDoc = true;
              if(!doc.containsKey(name)) {
                doc.setField(name, Lists.newArrayList());
              }
              doc.addField(name, readDoc(parser));
              break;
            }
            if (subDocs == null)
              subDocs = Lists.newArrayList();
            subDocs.add(readDoc(parser));
          }
          else {
            if (!"field".equals(localName)) {
              String msg = "XML element <doc> has invalid XML child element: " + localName;
              log.warn(msg);
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                      msg);
            }
            update = null;
            isNull = false;
            String attrVal = "";
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              attrName = parser.getAttributeLocalName(i);
              attrVal = parser.getAttributeValue(i);
              if (NAME.equals(attrName)) {
                name = attrVal;
              } else if ("boost".equals(attrName)) {
                String message = "Ignoring field boost: " + attrVal + " as index-time boosts are not supported anymore";
                if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
                  log.warn(message);
                } else {
                  log.debug(message);
                }
              } else if ("null".equals(attrName)) {
                isNull = StrUtils.parseBoolean(attrVal);
              } else if ("update".equals(attrName)) {
                update = attrVal;
              } else {
                log.warn("XML element <field> has invalid XML attr: {}", attrName);
              }
            }
          }
          break;
      }
    }

    if (updateMap != null)  {
      for (Map.Entry<String, Map<String, Object>> entry : updateMap.entrySet()) {
        name = entry.getKey();
        Map<String, Object> value = entry.getValue();
        doc.addField(name, value);
      }
    }

    return doc;
  }
}
