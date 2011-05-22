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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Stack;

import org.apache.commons.io.IOUtils;
import org.apache.noggit.JSONParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @since solr 4.0
 */
class JsonLoader extends ContentStreamLoader {
  final static Logger log = LoggerFactory.getLogger( JsonLoader.class );
  
  protected final UpdateRequestProcessor processor;
  protected final SolrQueryRequest req;
  protected JSONParser parser;
  protected final int commitWithin;
  protected final boolean overwrite;

  public JsonLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    this.processor = processor;
    this.req = req;

    commitWithin = req.getParams().getInt(XmlUpdateRequestHandler.COMMIT_WITHIN, -1);
    overwrite = req.getParams().getBool(XmlUpdateRequestHandler.OVERWRITE, true);
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception {
    errHeader = "JSONLoader: " + stream.getSourceInfo();
    Reader reader = null;
    try {
      reader = stream.getReader();
      if (log.isTraceEnabled()) {
        String body = IOUtils.toString(reader);
        log.trace("body", body);
        reader = new StringReader(body);
      }

      parser = new JSONParser(reader);
      this.processUpdate();
    }
    finally {
      IOUtils.closeQuietly(reader);
    }
  }

  @SuppressWarnings("fallthrough")
  void processUpdate() throws IOException
  {
    int ev = parser.nextEvent();
    while( ev != JSONParser.EOF ) {
      
      switch( ev )
      {
        case JSONParser.ARRAY_START:
          handleAdds();
          break;

      case JSONParser.STRING:
        if( parser.wasKey() ) {
          String v = parser.getString();
          if( v.equals( XmlUpdateRequestHandler.ADD ) ) {
            int ev2 = parser.nextEvent();
            if (ev2 == JSONParser.OBJECT_START) {
              processor.processAdd( parseAdd() );
            } else if (ev2 == JSONParser.ARRAY_START) {
              handleAdds();
            } else {
              assertEvent(ev2, JSONParser.OBJECT_START);
            }
          }
          else if( v.equals( XmlUpdateRequestHandler.COMMIT ) ) {
            CommitUpdateCommand cmd = new CommitUpdateCommand(req,  false );
            cmd.waitFlush = cmd.waitSearcher = true;
            parseCommitOptions( cmd );
            processor.processCommit( cmd );
          }
          else if( v.equals( XmlUpdateRequestHandler.OPTIMIZE ) ) {
            CommitUpdateCommand cmd = new CommitUpdateCommand(req, true );
            cmd.waitFlush = cmd.waitSearcher = true;
            parseCommitOptions( cmd );
            processor.processCommit( cmd );
          }
          else if( v.equals( XmlUpdateRequestHandler.DELETE ) ) {
            processor.processDelete( parseDelete() );
          }
          else if( v.equals( XmlUpdateRequestHandler.ROLLBACK ) ) {
            processor.processRollback( parseRollback() );
          }
          else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown command: "+v+" ["+parser.getPosition()+"]" );
          }
          break;
        }
        // fall through

      case JSONParser.LONG:
      case JSONParser.NUMBER:
      case JSONParser.BIGNUMBER:
      case JSONParser.BOOLEAN:
        log.info( "can't have a value here! "
            +JSONParser.getEventString(ev)+" "+parser.getPosition() );
        
      case JSONParser.OBJECT_START:
      case JSONParser.OBJECT_END:
      case JSONParser.ARRAY_END:
        break;
        
      default:
        log.info("Noggit UNKNOWN_EVENT_ID:"+ev);
        break;
      }
      // read the next event
      ev = parser.nextEvent();
    }
  }

  DeleteUpdateCommand parseDelete() throws IOException {
    assertNextEvent( JSONParser.OBJECT_START );

    DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);

    while( true ) {
      int ev = parser.nextEvent();
      if( ev == JSONParser.STRING ) {
        String key = parser.getString();
        if( parser.wasKey() ) {
          if( "id".equals( key ) ) {
            cmd.id = parser.getString();
          }
          else if( "query".equals(key) ) {
            cmd.query = parser.getString();
          }
          else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key: "+key+" ["+parser.getPosition()+"]" );
          }
        }
        else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "invalid string: " + key 
              +" at ["+parser.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        if( cmd.id == null && cmd.query == null ) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing id or query for delete ["+parser.getPosition()+"]" );
        }
        return cmd;
      }
      else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+parser.getPosition()+"]" );
      }
    }
  }
  
  RollbackUpdateCommand parseRollback() throws IOException {
    assertNextEvent( JSONParser.OBJECT_START );
    assertNextEvent( JSONParser.OBJECT_END );
    return new RollbackUpdateCommand(req);
  }

  void parseCommitOptions(CommitUpdateCommand cmd ) throws IOException
  {
    assertNextEvent( JSONParser.OBJECT_START );

    while( true ) {
      int ev = parser.nextEvent();
      if( ev == JSONParser.STRING ) {
        String key = parser.getString();
        if( parser.wasKey() ) {
          if( XmlUpdateRequestHandler.WAIT_SEARCHER.equals( key ) ) {
            cmd.waitSearcher = parser.getBoolean();
          }
          else if( XmlUpdateRequestHandler.WAIT_FLUSH.equals( key ) ) {
            cmd.waitFlush = parser.getBoolean();
          }
          else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key: "+key+" ["+parser.getPosition()+"]" );
          }
        }
        else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "invalid string: " + key 
              +" at ["+parser.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        return;
      }
      else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+parser.getPosition()+"]" );
      }
    }
  }
  
  AddUpdateCommand parseAdd() throws IOException
  {
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.commitWithin = commitWithin;
    cmd.overwrite = overwrite;

    float boost = 1.0f;
    
    while( true ) {
      int ev = parser.nextEvent();
      if( ev == JSONParser.STRING ) {
        if( parser.wasKey() ) {
          String key = parser.getString();
          if( "doc".equals( key ) ) {
            if( cmd.solrDoc != null ) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "multiple docs in same add command" );
            }
            ev = assertNextEvent( JSONParser.OBJECT_START );
            cmd.solrDoc = parseDoc( ev );
          }
          else if( XmlUpdateRequestHandler.OVERWRITE.equals( key ) ) {
            cmd.overwrite = parser.getBoolean(); // reads next boolean
          }
          else if( XmlUpdateRequestHandler.COMMIT_WITHIN.equals( key ) ) {
            cmd.commitWithin = (int)parser.getLong();
          }
          else if( "boost".equals( key ) ) {
            boost = Float.parseFloat( parser.getNumberChars().toString() );
          }
          else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key: "+key+" ["+parser.getPosition()+"]" );
          }
        }
        else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Should be a key "
              +" at ["+parser.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        if( cmd.solrDoc == null ) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"missing solr document. "+parser.getPosition() );
        }
        cmd.solrDoc.setDocumentBoost( boost ); 
        return cmd;
      }
      else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+parser.getPosition()+"]" );
      }
    }
  }


  void handleAdds() throws IOException
  {
    while( true ) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.commitWithin = commitWithin;
      cmd.overwrite = overwrite;

      int ev = parser.nextEvent();
      if (ev == JSONParser.ARRAY_END) break;

      assertEvent(ev, JSONParser.OBJECT_START);
      cmd.solrDoc = parseDoc(ev);
      processor.processAdd(cmd);
    }
  }


  int assertNextEvent(int expected ) throws IOException
  {
    int got = parser.nextEvent();
    assertEvent(got, expected);
    return got;
  }

  void assertEvent(int ev, int expected) {
    if( ev != expected ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected: "+JSONParser.getEventString( expected  )
          +" but got "+JSONParser.getEventString( ev )
          +" at ["+parser.getPosition()+"]" );
    }
  }
  
  SolrInputDocument parseDoc(int ev) throws IOException
  {
    Stack<Object> stack = new Stack<Object>();
    Object obj = null;
    boolean inArray = false;
    
    if( ev != JSONParser.OBJECT_START ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "object should already be started" );
    }
    
    while( true ) {
      //System.out.println( ev + "["+JSONParser.getEventString(ev)+"] "+parser.wasKey() ); //+ parser.getString() );

      switch (ev) {
        case JSONParser.STRING:
          if( parser.wasKey() ) {
            obj = stack.peek();
            String v = parser.getString();
            if( obj instanceof SolrInputField ) {
              SolrInputField field = (SolrInputField)obj;
              if( "boost".equals( v ) ) {
                ev = parser.nextEvent();
                if( ev != JSONParser.NUMBER &&
                    ev != JSONParser.LONG &&  
                    ev != JSONParser.BIGNUMBER ) {
                  throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "boost should have number! "+JSONParser.getEventString(ev) );
                }
                field.setBoost( Float.valueOf( parser.getNumberChars().toString() ) );
              }
              else if( "value".equals( v  ) ) {
                // nothing special...
                stack.push( field ); // so it can be popped
              }
              else {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid key: "+v + " ["+ parser.getPosition()+"]" );
              }
            }
            else if( obj instanceof SolrInputDocument ) {
              SolrInputDocument doc = (SolrInputDocument)obj;
              SolrInputField f = doc.get( v );
              if( f == null ) {
                f = new SolrInputField( v );
                doc.put( f.getName(), f );
              }
              stack.push( f );
            }
            else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "hymmm ["+ parser.getPosition()+"]" );
            }
          }
          else {
            addValToField(stack, parser.getString(), inArray, parser);
          }
          break;

        case JSONParser.LONG:
        case JSONParser.NUMBER:
        case JSONParser.BIGNUMBER:
          addValToField(stack, parser.getNumberChars().toString(), inArray, parser);
          break;
          
        case JSONParser.BOOLEAN:
          addValToField(stack, parser.getBoolean(),inArray, parser);
          break;
          
        case JSONParser.OBJECT_START:
          if( stack.isEmpty() ) {
            stack.push( new SolrInputDocument() );
          }
          else {
            obj = stack.peek();
            if( obj instanceof SolrInputField ) {
              // should alreay be pushed...
            }
            else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "should not start new object with: "+obj + " ["+ parser.getPosition()+"]" );
            }
          }
          break;
          
        case JSONParser.OBJECT_END:
          obj = stack.pop();
          if( obj instanceof SolrInputDocument ) {
            return (SolrInputDocument)obj;
          }
          else if( obj instanceof SolrInputField ) {
            // should already be pushed...
          }
          else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "should not start new object with: "+obj + " ["+ parser.getPosition()+"]" );
          }
          break;

        case JSONParser.ARRAY_START:
          inArray = true;
          break;
          
        case JSONParser.ARRAY_END:
          inArray = false;
          stack.pop(); // the val should have done it...
          break;
          
        default:
          System.out.println("UNKNOWN_EVENT_ID:"+ev);
          break;
      }

      ev = parser.nextEvent();
      if( ev == JSONParser.EOF ) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "should finish doc first!" );
      }
    }
  }
  
  static void addValToField( Stack stack, Object val, boolean inArray, JSONParser parser ) throws IOException
  {
    Object obj = stack.peek();
    if( !(obj instanceof SolrInputField) ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "hymmm ["+parser.getPosition()+"]" );
    }
    
    SolrInputField f = inArray
      ? (SolrInputField)obj
      : (SolrInputField)stack.pop();
   
    float boost = (f.getValue()==null)?f.getBoost():1.0f;
    f.addValue( val,boost );
  }


}
