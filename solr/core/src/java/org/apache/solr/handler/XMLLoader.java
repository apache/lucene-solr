package org.apache.solr.handler;
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

import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.commons.io.IOUtils;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.TransformerConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;


/**
 *
 *
 **/
class XMLLoader extends ContentStreamLoader {
  protected UpdateRequestProcessor processor;
  protected XMLInputFactory inputFactory;

  public XMLLoader(UpdateRequestProcessor processor, XMLInputFactory inputFactory) {
    this.processor = processor;
    this.inputFactory = inputFactory;
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception {
    errHeader = "XMLLoader: " + stream.getSourceInfo();
    InputStream is = null;
    XMLStreamReader parser = null;
    try {
      is = stream.getStream();
      final String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
      if (XmlUpdateRequestHandler.log.isTraceEnabled()) {
        final byte[] body = IOUtils.toByteArray(is);
        // TODO: The charset may be wrong, as the real charset is later
        // determined by the XML parser, the content-type is only used as a hint!
        XmlUpdateRequestHandler.log.trace("body", new String(body, (charset == null) ?
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




  /**
   * @since solr 1.2
   */
  void processUpdate(SolrQueryRequest req, UpdateRequestProcessor processor, XMLStreamReader parser)
          throws XMLStreamException, IOException, FactoryConfigurationError,
          InstantiationException, IllegalAccessException,
          TransformerConfigurationException {
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
          if (currTag.equals(XmlUpdateRequestHandler.ADD)) {
            XmlUpdateRequestHandler.log.trace("SolrCore.update(add)");

            addCmd = new AddUpdateCommand(req);

            // First look for commitWithin parameter on the request, will be overwritten for individual <add>'s
            addCmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
            
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (XmlUpdateRequestHandler.OVERWRITE.equals(attrName)) {
                addCmd.overwrite = StrUtils.parseBoolean(attrVal);
              } else if (XmlUpdateRequestHandler.COMMIT_WITHIN.equals(attrName)) {
                addCmd.commitWithin = Integer.parseInt(attrVal);
              } else {
                XmlUpdateRequestHandler.log.warn("Unknown attribute id in add:" + attrName);
              }
            }

          } else if ("doc".equals(currTag)) {
            if(addCmd != null) {
              XmlUpdateRequestHandler.log.trace("adding doc...");
              addCmd.clear();
              addCmd.solrDoc = readDoc(parser);
              processor.processAdd(addCmd);
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unexpected <doc> tag without an <add> tag surrounding it.");
            }
          } else if (XmlUpdateRequestHandler.COMMIT.equals(currTag) || XmlUpdateRequestHandler.OPTIMIZE.equals(currTag)) {
            XmlUpdateRequestHandler.log.trace("parsing " + currTag);

            CommitUpdateCommand cmd = new CommitUpdateCommand(req, XmlUpdateRequestHandler.OPTIMIZE.equals(currTag));

            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (XmlUpdateRequestHandler.WAIT_SEARCHER.equals(attrName)) {
                cmd.waitSearcher = StrUtils.parseBoolean(attrVal);
              } else if (XmlUpdateRequestHandler.SOFT_COMMIT.equals(attrName)) {
                cmd.softCommit = StrUtils.parseBoolean(attrVal);
              } else if (UpdateParams.MAX_OPTIMIZE_SEGMENTS.equals(attrName)) {
                cmd.maxOptimizeSegments = Integer.parseInt(attrVal);
              } else if (UpdateParams.EXPUNGE_DELETES.equals(attrName)) {
                cmd.expungeDeletes = StrUtils.parseBoolean(attrVal);
              } else {
                XmlUpdateRequestHandler.log.warn("unexpected attribute commit/@" + attrName);
              }
            }

            processor.processCommit(cmd);
          } // end commit
          else if (XmlUpdateRequestHandler.ROLLBACK.equals(currTag)) {
            XmlUpdateRequestHandler.log.trace("parsing " + currTag);

            RollbackUpdateCommand cmd = new RollbackUpdateCommand(req);

            processor.processRollback(cmd);
          } // end rollback
          else if (XmlUpdateRequestHandler.DELETE.equals(currTag)) {
            XmlUpdateRequestHandler.log.trace("parsing delete");
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

    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String attrName = parser.getAttributeLocalName(i);
      String attrVal = parser.getAttributeValue(i);
      if ("fromPending".equals(attrName)) {
        // deprecated
      } else if ("fromCommitted".equals(attrName)) {
        // deprecated
      } else {
        XmlUpdateRequestHandler.log.warn("unexpected attribute delete/@" + attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          String mode = parser.getLocalName();
          if (!("id".equals(mode) || "query".equals(mode))) {
            XmlUpdateRequestHandler.log.warn("unexpected XML tag /delete/" + mode);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag /delete/" + mode);
          }
          text.setLength(0);
          break;

        case XMLStreamConstants.END_ELEMENT:
          String currTag = parser.getLocalName();
          if ("id".equals(currTag)) {
            deleteCmd.id = text.toString();
          } else if ("query".equals(currTag)) {
            deleteCmd.query = text.toString();
          } else if ("delete".equals(currTag)) {
            return;
          } else {
            XmlUpdateRequestHandler.log.warn("unexpected XML tag /delete/" + currTag);
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
  SolrInputDocument readDoc(XMLStreamReader parser) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();

    String attrName = "";
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      attrName = parser.getAttributeLocalName(i);
      if ("boost".equals(attrName)) {
        doc.setDocumentBoost(Float.parseFloat(parser.getAttributeValue(i)));
      } else {
        XmlUpdateRequestHandler.log.warn("Unknown attribute doc/@" + attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    String name = null;
    float boost = 1.0f;
    boolean isNull = false;
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
            if (!isNull) {
              doc.addField(name, text.toString(), boost);
              boost = 1.0f;
            }
          }
          break;

        case XMLStreamConstants.START_ELEMENT:
          text.setLength(0);
          String localName = parser.getLocalName();
          if (!"field".equals(localName)) {
            XmlUpdateRequestHandler.log.warn("unexpected XML tag doc/" + localName);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag doc/" + localName);
          }
          boost = 1.0f;
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
            } else {
              XmlUpdateRequestHandler.log.warn("Unknown attribute doc/field/@" + attrName);
            }
          }
          break;
      }
    }
  }

}
