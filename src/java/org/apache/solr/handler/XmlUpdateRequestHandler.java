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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.logging.Logger;

import javanet.staxutils.BaseXMLInputFactory;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.TransformerConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 * Add documents to solr using the STAX XML parser.
 */
public class XmlUpdateRequestHandler extends RequestHandlerBase
{
  public static Logger log = Logger.getLogger(XmlUpdateRequestHandler.class.getName());

  public static final String UPDATE_PROCESSOR = "update.processor";

  // XML Constants
  public static final String ADD = "add";
  public static final String DELETE = "delete";
  public static final String OPTIMIZE = "optimize";
  public static final String COMMIT = "commit";
  public static final String WAIT_SEARCHER = "waitSearcher";
  public static final String WAIT_FLUSH = "waitFlush";
  
  public static final String OVERWRITE = "overwrite";
  public static final String OVERWRITE_COMMITTED = "overwriteCommitted"; // @Deprecated
  public static final String OVERWRITE_PENDING = "overwritePending";  // @Deprecated
  public static final String ALLOW_DUPS = "allowDups"; 
  
  private XMLInputFactory inputFactory;
  
  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args)
  {
    super.init(args);
    inputFactory = BaseXMLInputFactory.newInstance();
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    RequestHandlerUtils.addExperimentalFormatWarning( rsp );
    
    SolrParams params = req.getParams();
    UpdateRequestProcessorFactory processorFactory = 
      req.getCore().getUpdateProcessorFactory( params.get( UpdateParams.UPDATE_PROCESSOR ) );
    
    UpdateRequestProcessor processor = processorFactory.getInstance(req, rsp, null);
    Iterable<ContentStream> streams = req.getContentStreams();
    if( streams == null ) {
      if( !RequestHandlerUtils.handleCommit(processor, params, false) ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "missing content stream" );
      }
    }
    else {
      // Cycle through each stream
      for( ContentStream stream : req.getContentStreams() ) {
        Reader reader = stream.getReader();
        try {
          XMLStreamReader parser = inputFactory.createXMLStreamReader(reader);
          this.processUpdate( processor, parser );
        }
        finally {
          IOUtils.closeQuietly(reader);
        }
      }
      
      // Perhaps commit from the parameters
      RequestHandlerUtils.handleCommit( processor, params, false );
    }
    
    // finish the request
    processor.finish(); 
  }
    
  /**
   * @since solr 1.2
   */
  void processUpdate( UpdateRequestProcessor processor, XMLStreamReader parser)
    throws XMLStreamException, IOException, FactoryConfigurationError,
          InstantiationException, IllegalAccessException,
          TransformerConfigurationException 
  {
    AddUpdateCommand addCmd = null;
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.END_DOCUMENT:
          parser.close();
          return;

        case XMLStreamConstants.START_ELEMENT:
          String currTag = parser.getLocalName();
          if (currTag.equals(ADD)) {
            log.finest("SolrCore.update(add)");
            
            addCmd = new AddUpdateCommand();
            boolean overwrite=true;  // the default

            Boolean overwritePending = null;
            Boolean overwriteCommitted = null;
            for (int i=0; i<parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (OVERWRITE.equals(attrName)) {
                overwrite = StrUtils.parseBoolean(attrVal);
              } else if (ALLOW_DUPS.equals(attrName)) {
                overwrite = !StrUtils.parseBoolean(attrVal);
              } else if ( OVERWRITE_PENDING.equals(attrName) ) {
                overwritePending = StrUtils.parseBoolean(attrVal);
              } else if ( OVERWRITE_COMMITTED.equals(attrName) ) {
                overwriteCommitted = StrUtils.parseBoolean(attrVal);
              } else {
                log.warning("Unknown attribute id in add:" + attrName);
              }
            }
            
            // check if these flags are set
            if( overwritePending != null && overwriteCommitted != null ) {
              if( overwritePending != overwriteCommitted ) {
                throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, 
                    "can't have different values for 'overwritePending' and 'overwriteCommitted'" );
              }
              overwrite=overwritePending;
            }
            addCmd.overwriteCommitted =  overwrite;
            addCmd.overwritePending   =  overwrite;
            addCmd.allowDups          = !overwrite;
          } 
          else if ("doc".equals(currTag)) {
            log.finest("adding doc...");
            addCmd.clear();
            addCmd.solrDoc = readDoc( parser );
            processor.processAdd(addCmd);
          } 
          else if ( COMMIT.equals(currTag) || OPTIMIZE.equals(currTag)) {
            log.finest("parsing " + currTag);

            CommitUpdateCommand cmd = new CommitUpdateCommand(OPTIMIZE.equals(currTag));

            boolean sawWaitSearcher = false, sawWaitFlush = false;
            for (int i = 0; i < parser.getAttributeCount(); i++) {
              String attrName = parser.getAttributeLocalName(i);
              String attrVal = parser.getAttributeValue(i);
              if (WAIT_FLUSH.equals(attrName)) {
                cmd.waitFlush = StrUtils.parseBoolean(attrVal);
                sawWaitFlush = true;
              } else if (WAIT_SEARCHER.equals(attrName)) {
                cmd.waitSearcher = StrUtils.parseBoolean(attrVal);
                sawWaitSearcher = true;
              } else {
                log.warning("unexpected attribute commit/@" + attrName);
              }
            }

            // If waitFlush is specified and waitSearcher wasn't, then
            // clear waitSearcher.
            if (sawWaitFlush && !sawWaitSearcher) {
              cmd.waitSearcher = false;
            }
            processor.processCommit( cmd );
          } // end commit
          else if (DELETE.equals(currTag)) {
            log.finest("parsing delete");
            processDelete( processor, parser);
          } // end delete
          break;
       }
    }
  }

  /**
   * @since solr 1.3
   */
  void processDelete(UpdateRequestProcessor processor, XMLStreamReader parser) throws XMLStreamException, IOException 
  {
    // Parse the command
    DeleteUpdateCommand deleteCmd = new DeleteUpdateCommand();
    deleteCmd.fromPending = true;
    deleteCmd.fromCommitted = true;
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String attrName = parser.getAttributeLocalName(i);
      String attrVal = parser.getAttributeValue(i);
      if ("fromPending".equals(attrName)) {
        deleteCmd.fromPending = StrUtils.parseBoolean(attrVal);
      } else if ("fromCommitted".equals(attrName)) {
        deleteCmd.fromCommitted = StrUtils.parseBoolean(attrVal);
      } else {
        log.warning("unexpected attribute delete/@" + attrName);
      }
    }

    StringBuilder text = new StringBuilder();
    while (true) {
      int event = parser.next();
      switch (event) {
      case XMLStreamConstants.START_ELEMENT:
        String mode = parser.getLocalName();
        if (!("id".equals(mode) || "query".equals(mode))) {
          log.warning("unexpected XML tag /delete/" + mode);
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
              "unexpected XML tag /delete/" + mode);
        }
        text.setLength( 0 );
        break;
        
      case XMLStreamConstants.END_ELEMENT:
        String currTag = parser.getLocalName();
        if ("id".equals(currTag)) {
          deleteCmd.id = text.toString();
        } else if ("query".equals(currTag)) {
          deleteCmd.query = text.toString();
        } else if( "delete".equals( currTag ) ) {
          return;
        } else {
          log.warning("unexpected XML tag /delete/" + currTag);
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
              "unexpected XML tag /delete/" + currTag);
        }
        processor.processDelete( deleteCmd );
        break;

      // Add everything to the text
      case XMLStreamConstants.SPACE:
      case XMLStreamConstants.CDATA:
      case XMLStreamConstants.CHARACTERS:
        text.append( parser.getText() );
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
        doc.setDocumentBoost(  Float.parseFloat(parser.getAttributeValue(i)) );
      } else {
        log.warning("Unknown attribute doc/@" + attrName);
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
        text.append( parser.getText() );
        break;
        
      case XMLStreamConstants.END_ELEMENT:
        if ("doc".equals(parser.getLocalName())) {
          return doc;
        } 
        else if ("field".equals(parser.getLocalName())) {
          if (!isNull) {
            doc.addField(name, text.toString(), boost );
            boost = 1.0f;
          }
        }
        break;
        
      case XMLStreamConstants.START_ELEMENT:
        text.setLength(0);
        String localName = parser.getLocalName();
        if (!"field".equals(localName)) {
          log.warning("unexpected XML tag doc/" + localName);
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
            log.warning("Unknown attribute doc/field/@" + attrName);
          }
        }
        break;
      }
    }
  }

  /**
   * A Convenience method for getting back a simple XML string indicating
   * success or failure from an XML formated Update (from the Reader)
   * 
   * @since solr 1.2
   */
  @Deprecated
  public void doLegacyUpdate(SolrCore core, Reader input, Writer output) {
    try {
      //SolrCore core = SolrCore.getSolrCore();

      // Old style requests do not choose a custom handler
      UpdateRequestProcessorFactory processorFactory = core.getUpdateProcessorFactory( null );
      
      SolrParams params = new MapSolrParams( new HashMap<String, String>() );
      SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
      SolrQueryResponse rsp = new SolrQueryResponse(); // ignored
      XMLStreamReader parser = inputFactory.createXMLStreamReader(input);
      UpdateRequestProcessor processor = processorFactory.getInstance(req, rsp, null);
      this.processUpdate( processor, parser );
      processor.finish();
      output.write("<result status=\"0\"></result>");
    } 
    catch (Exception ex) {
      try {
        XML.writeXML(output, "result", SolrException.toStr(ex), "status", "1");
      } catch (Exception ee) {
        log.severe("Error writing to output stream: " + ee);
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



