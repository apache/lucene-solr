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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.UpdateHandler;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

/**
 * This is an XPP implementaion of the XmlUpdateRequestHandler -- it is getting
 * replaced with a more flexible StAX implementation.  This will be remove 
 * before the next official release: solr 1.3
 */
@Deprecated
public class XppUpdateRequestHandler extends RequestHandlerBase
{
  public static Logger log = Logger.getLogger(XppUpdateRequestHandler.class.getName());

  private XmlPullParserFactory factory;

  @Override
  public void init(NamedList args)
  {
    super.init( args );
    
    try {
      factory = XmlPullParserFactory.newInstance();
      factory.setNamespaceAware(false);
    } 
    catch (XmlPullParserException e) {
       throw new RuntimeException(e);
    }
  }
    
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    Iterable<ContentStream> streams = req.getContentStreams();
    if( streams == null ) {
      if( !RequestHandlerUtils.handleCommit(req, rsp, false) ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "missing content stream" );
      }
      return;
    }

    // Cycle through each stream
    for( ContentStream stream : req.getContentStreams() ) {
      Reader reader = stream.getReader();
      try {
        NamedList out = this.update( reader );
        // TODO -- return useful info.  
        // rsp.add( "update", out );
      }
      finally {
        IOUtils.closeQuietly(reader);
      }
    }
    
    // perhaps commit when we are done
    RequestHandlerUtils.handleCommit(req, rsp, false);
  }


  public NamedList update( Reader reader ) throws Exception
  {
    SolrCore core = SolrCore.getSolrCore();
    IndexSchema schema = core.getSchema();
    UpdateHandler updateHandler = core.getUpdateHandler();
    
    // TODO: What results should be returned?
    SimpleOrderedMap res = new SimpleOrderedMap();

    XmlPullParser xpp = factory.newPullParser();
    long startTime=System.currentTimeMillis();

      xpp.setInput(reader);
      xpp.nextTag();

      String currTag = xpp.getName();
      if ("add".equals(currTag)) {
        log.finest("SolrCore.update(add)");
        AddUpdateCommand cmd = new AddUpdateCommand();
        cmd.allowDups=false;  // the default

        int status=0;
        boolean pendingAttr=false, committedAttr=false;
        int attrcount = xpp.getAttributeCount();
        for (int i=0; i<attrcount; i++) {
          String attrName = xpp.getAttributeName(i);
          String attrVal = xpp.getAttributeValue(i);
          if ("allowDups".equals(attrName)) {
            cmd.allowDups = StrUtils.parseBoolean(attrVal);
          } else if ("overwritePending".equals(attrName)) {
            cmd.overwritePending = StrUtils.parseBoolean(attrVal);
            pendingAttr=true;
          } else if ("overwriteCommitted".equals(attrName)) {
            cmd.overwriteCommitted = StrUtils.parseBoolean(attrVal);
            committedAttr=true;
          } else {
            log.warning("Unknown attribute id in add:" + attrName);
          }
        }

        //set defaults for committed and pending based on allowDups value
        if (!pendingAttr) cmd.overwritePending=!cmd.allowDups;
        if (!committedAttr) cmd.overwriteCommitted=!cmd.allowDups;

        DocumentBuilder builder = new DocumentBuilder(schema);
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        int eventType=0;
        // accumulate responses
        List<String> added = new ArrayList<String>(10);
        while(true) {
          // this may be our second time through the loop in the case
          // that there are multiple docs in the add... so make sure that
          // objects can handle that.

          cmd.indexedId = null;  // reset the id for this add

          if (eventType !=0) {
            eventType=xpp.getEventType();
            if (eventType==XmlPullParser.END_DOCUMENT) break;
          }
          // eventType = xpp.next();
          eventType = xpp.nextTag();
          if (eventType == XmlPullParser.END_TAG || eventType == XmlPullParser.END_DOCUMENT) break;  // should match </add>

          readDoc(builder,xpp);
          builder.endDoc();
          cmd.doc = builder.getDoc();
          log.finest("adding doc...");
          updateHandler.addDoc(cmd);
          String docId = null;
          if (uniqueKeyField!=null)
            docId = schema.printableUniqueKey(cmd.doc);
          added.add(docId);
          
        } // end while
        // write log and result
        StringBuilder out = new StringBuilder();
        for (String docId: added)
          if(docId != null)
            out.append(docId + ",");
        String outMsg = out.toString();
        if(outMsg.length() > 0)
          outMsg = outMsg.substring(0, outMsg.length() - 1);
        log.info("added id={" + outMsg  + "} in " + (System.currentTimeMillis()-startTime) + "ms");
        
        // Add output 
        res.add( "added", outMsg );
    } // end add

      else if ("commit".equals(currTag) || "optimize".equals(currTag)) {
        log.finest("parsing "+currTag);
        
          CommitUpdateCommand cmd = new CommitUpdateCommand("optimize".equals(currTag));

          boolean sawWaitSearcher=false, sawWaitFlush=false;
          int attrcount = xpp.getAttributeCount();
          for (int i=0; i<attrcount; i++) {
            String attrName = xpp.getAttributeName(i);
            String attrVal = xpp.getAttributeValue(i);
            if ("waitFlush".equals(attrName)) {
              cmd.waitFlush = StrUtils.parseBoolean(attrVal);
              sawWaitFlush=true;
            } else if ("waitSearcher".equals(attrName)) {
              cmd.waitSearcher = StrUtils.parseBoolean(attrVal);
              sawWaitSearcher=true;
            } else {
              log.warning("unexpected attribute commit/@" + attrName);
            }
          }

          // If waitFlush is specified and waitSearcher wasn't, then
          // clear waitSearcher.
          if (sawWaitFlush && !sawWaitSearcher) {
            cmd.waitSearcher=false;
          }

          updateHandler.commit(cmd);
          if ("optimize".equals(currTag)) {
            log.info("optimize 0 "+(System.currentTimeMillis()-startTime));
          }
          else {
            log.info("commit 0 "+(System.currentTimeMillis()-startTime));
          }
          while (true) {
            int eventType = xpp.nextTag();
            if (eventType == XmlPullParser.END_TAG) break; // match </commit>
          }
          
          // add debug output
          res.add( cmd.optimize?"optimize":"commit", "" );
      }  // end commit

    else if ("delete".equals(currTag)) {
      log.finest("parsing delete");

        DeleteUpdateCommand cmd = new DeleteUpdateCommand();
        cmd.fromPending=true;
        cmd.fromCommitted=true;
        int attrcount = xpp.getAttributeCount();
        for (int i=0; i<attrcount; i++) {
          String attrName = xpp.getAttributeName(i);
          String attrVal = xpp.getAttributeValue(i);
          if ("fromPending".equals(attrName)) {
            cmd.fromPending = StrUtils.parseBoolean(attrVal);
          } else if ("fromCommitted".equals(attrName)) {
            cmd.fromCommitted = StrUtils.parseBoolean(attrVal);
          } else {
            log.warning("unexpected attribute delete/@" + attrName);
          }
        }

        int eventType = xpp.nextTag();
        currTag = xpp.getName();
        String val = xpp.nextText();

        if ("id".equals(currTag)) {
          cmd.id =  val;
          updateHandler.delete(cmd);
          log.info("delete(id " + val + ") 0 " +
                   (System.currentTimeMillis()-startTime));
        } else if ("query".equals(currTag)) {
          cmd.query =  val;
          updateHandler.deleteByQuery(cmd);
          log.info("deleteByQuery(query " + val + ") 0 " +
                   (System.currentTimeMillis()-startTime));
        } else {
          log.warning("unexpected XML tag /delete/"+currTag);
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unexpected XML tag /delete/"+currTag);
        }

          res.add( "delete", "" );

        while (xpp.nextTag() != XmlPullParser.END_TAG);
      } // end delete
      return res;
  }

  private void readDoc(DocumentBuilder builder, XmlPullParser xpp) throws IOException, XmlPullParserException {
    // xpp should be at <doc> at this point

    builder.startDoc();

    int attrcount = xpp.getAttributeCount();
    float docBoost = 1.0f;

    for (int i=0; i<attrcount; i++) {
      String attrName = xpp.getAttributeName(i);
      String attrVal = xpp.getAttributeValue(i);
      if ("boost".equals(attrName)) {
        docBoost = Float.parseFloat(attrVal);
        builder.setBoost(docBoost);
      } else {
        log.warning("Unknown attribute doc/@" + attrName);
      }
    }
    if (docBoost != 1.0f) builder.setBoost(docBoost);

    // while (findNextTag(xpp,"field") != XmlPullParser.END_DOCUMENT) {

    while(true) {
      int eventType = xpp.nextTag();
      if (eventType == XmlPullParser.END_TAG) break;  // </doc>

      String tname=xpp.getName();
      // System.out.println("FIELD READER AT TAG " + tname);

      if (!"field".equals(tname)) {
        log.warning("unexpected XML tag doc/"+tname);
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unexpected XML tag doc/"+tname);
      }

      //
      // get field name and parse field attributes
      //
      attrcount = xpp.getAttributeCount();
      String name=null;
      float boost=1.0f;
      boolean isNull=false;

      for (int i=0; i<attrcount; i++) {
        String attrName = xpp.getAttributeName(i);
        String attrVal = xpp.getAttributeValue(i);
        if ("name".equals(attrName)) {
          name=attrVal;
        } else if ("boost".equals(attrName)) {
          boost=Float.parseFloat(attrVal);
        } else if ("null".equals(attrName)) {
          isNull=StrUtils.parseBoolean(attrVal);
        } else {
          log.warning("Unknown attribute doc/field/@" + attrName);
        }
      }

      // now get the field value
      String val = xpp.nextText();      // todo... text event for <field></field>???
                                        // need this line for isNull???
      // Don't add fields marked as null (for now at least)
      if (!isNull) {
        if (boost != 1.0f) {
          builder.addField(name,val,boost);
        } else {
          builder.addField(name,val);
        }
      }

      // do I have to do a nextTag here to read the end_tag?

    } // end field loop
  }

  /**
   * A Convenience method for getting back a simple XML string indicating
   * successs or failure from an XML formated Update (from the Reader)
   */
  public void doLegacyUpdate(Reader input, Writer output) {
    
    try {
      NamedList ignored = this.update( input );
      output.write("<result status=\"0\"></result>");
    }
    catch( Exception ex ) {
      try {
        XML.writeXML(output,"result",SolrException.toStr(ex),"status","1");
      } catch (Exception ee) {
        log.severe("Error writing to output stream: "+ee);
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
