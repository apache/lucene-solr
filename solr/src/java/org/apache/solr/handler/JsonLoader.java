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
  
  protected UpdateRequestProcessor processor;

  public JsonLoader(UpdateRequestProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception {
    errHeader = "JSONLoader: " + stream.getSourceInfo();
    Reader reader = null;
    try {
      reader = stream.getReader();
      if (XmlUpdateRequestHandler.log.isTraceEnabled()) {
        String body = IOUtils.toString(reader);
        XmlUpdateRequestHandler.log.trace("body", body);
        reader = new StringReader(body);
      }

      JSONParser parser = new JSONParser(reader);
      this.processUpdate(req, processor, parser);
    }
    finally {
      IOUtils.closeQuietly(reader);
    }
  }

  @SuppressWarnings("fallthrough")
  void processUpdate(SolrQueryRequest req, UpdateRequestProcessor processor, JSONParser parser) throws IOException 
  {
    int ev = parser.nextEvent();
    while( ev != JSONParser.EOF ) {
      
      switch( ev )
      {
      case JSONParser.STRING:
        if( parser.wasKey() ) {
          String v = parser.getString();
          if( v.equals( XmlUpdateRequestHandler.ADD ) ) {
            processor.processAdd( parseAdd(req, parser ) );
          }
          else if( v.equals( XmlUpdateRequestHandler.COMMIT ) ) {
            CommitUpdateCommand cmd = new CommitUpdateCommand(req,  false );
            cmd.waitFlush = cmd.waitSearcher = true;
            parseCommitOptions( parser, cmd );
            processor.processCommit( cmd );
          }
          else if( v.equals( XmlUpdateRequestHandler.OPTIMIZE ) ) {
            CommitUpdateCommand cmd = new CommitUpdateCommand(req, true );
            cmd.waitFlush = cmd.waitSearcher = true;
            parseCommitOptions( parser, cmd );
            processor.processCommit( cmd );
          }
          else if( v.equals( XmlUpdateRequestHandler.DELETE ) ) {
            processor.processDelete( parseDelete(req, parser ) );
          }
          else if( v.equals( XmlUpdateRequestHandler.ROLLBACK ) ) {
            processor.processRollback( parseRollback(req, parser ) );
          }
          else {
            throw new IOException( "Unknown command: "+v+" ["+parser.getPosition()+"]" );
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
      case JSONParser.ARRAY_START:
      case JSONParser.ARRAY_END:
        break;
        
      default:
        System.out.println("UNKNOWN_EVENT_ID:"+ev);
        break;
      }
      // read the next event
      ev = parser.nextEvent();
    }
  }

  DeleteUpdateCommand parseDelete(SolrQueryRequest req, JSONParser js) throws IOException {
    assertNextEvent( js, JSONParser.OBJECT_START );

    DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
    
    while( true ) {
      int ev = js.nextEvent();
      if( ev == JSONParser.STRING ) {
        String key = js.getString();
        if( js.wasKey() ) {
          if( "id".equals( key ) ) {
            cmd.id = js.getString();
          }
          else if( "query".equals(key) ) {
            cmd.query = js.getString();
          }
          else {
            throw new IOException( "Unknown key: "+key+" ["+js.getPosition()+"]" );
          }
        }
        else {
          throw new IOException( 
              "invalid string: " + key 
              +" at ["+js.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        if( cmd.id == null && cmd.query == null ) {
          throw new IOException( "Missing id or query for delete ["+js.getPosition()+"]" );          
        }
        return cmd;
      }
      else {
        throw new IOException( 
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+js.getPosition()+"]" );
      }
    }
  }
  
  RollbackUpdateCommand parseRollback(SolrQueryRequest req, JSONParser js) throws IOException {
    assertNextEvent( js, JSONParser.OBJECT_START );
    assertNextEvent( js, JSONParser.OBJECT_END );
    return new RollbackUpdateCommand(req);
  }

  void parseCommitOptions( JSONParser js, CommitUpdateCommand cmd ) throws IOException
  {
    assertNextEvent( js, JSONParser.OBJECT_START );

    while( true ) {
      int ev = js.nextEvent();
      if( ev == JSONParser.STRING ) {
        String key = js.getString();
        if( js.wasKey() ) {
          if( XmlUpdateRequestHandler.WAIT_SEARCHER.equals( key ) ) {
            cmd.waitSearcher = js.getBoolean();
          }
          else if( XmlUpdateRequestHandler.WAIT_FLUSH.equals( key ) ) {
            cmd.waitFlush = js.getBoolean();
          }
          else {
            throw new IOException( "Unknown key: "+key+" ["+js.getPosition()+"]" );
          }
        }
        else {
          throw new IOException( 
              "invalid string: " + key 
              +" at ["+js.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        return;
      }
      else {
        throw new IOException( 
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+js.getPosition()+"]" );
      }
    }
  }
  
  AddUpdateCommand parseAdd(SolrQueryRequest req, JSONParser js ) throws IOException
  {
    assertNextEvent( js, JSONParser.OBJECT_START );
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    float boost = 1.0f;
    
    while( true ) {
      int ev = js.nextEvent();
      if( ev == JSONParser.STRING ) {
        if( js.wasKey() ) {
          String key = js.getString();
          if( "doc".equals( key ) ) {
            if( cmd.solrDoc != null ) {
              throw new IOException( "multiple docs in same add command" );
            }
            ev = assertNextEvent( js, JSONParser.OBJECT_START );
            cmd.solrDoc = parseDoc( ev, js );
          }
          else if( XmlUpdateRequestHandler.OVERWRITE.equals( key ) ) {
            cmd.overwrite = js.getBoolean(); // reads next boolean
          }
          else if( XmlUpdateRequestHandler.COMMIT_WITHIN.equals( key ) ) {
            cmd.commitWithin = (int)js.getLong(); 
          }
          else if( "boost".equals( key ) ) {
            boost = Float.parseFloat( js.getNumberChars().toString() ); 
          }
          else {
            throw new IOException( "Unknown key: "+key+" ["+js.getPosition()+"]" );
          }
        }
        else {
          throw new IOException( 
              "Should be a key "
              +" at ["+js.getPosition()+"]" );
        }
      }
      else if( ev == JSONParser.OBJECT_END ) {
        if( cmd.solrDoc == null ) {
          throw new IOException("missing solr document. "+js.getPosition() );
        }
        cmd.solrDoc.setDocumentBoost( boost ); 
        return cmd;
      }
      else {
        throw new IOException( 
            "Got: "+JSONParser.getEventString( ev  )
            +" at ["+js.getPosition()+"]" );
      }
    }
  }
  
  int assertNextEvent( JSONParser parser, int ev ) throws IOException
  {
    int got = parser.nextEvent();
    if( ev != got ) {
      throw new IOException( 
          "Expected: "+JSONParser.getEventString( ev  )
          +" but got "+JSONParser.getEventString( got )
          +" at ["+parser.getPosition()+"]" );
    }
    return got;
  }
  
  SolrInputDocument parseDoc( int ev, JSONParser js ) throws IOException
  {
    Stack<Object> stack = new Stack<Object>();
    Object obj = null;
    boolean inArray = false;
    
    if( ev != JSONParser.OBJECT_START ) {
      throw new IOException( "object should already be started" );
    }
    
    while( true ) {
      //System.out.println( ev + "["+JSONParser.getEventString(ev)+"] "+js.wasKey() ); //+ js.getString() );

      switch (ev) {
        case JSONParser.STRING:
          if( js.wasKey() ) {
            obj = stack.peek();
            String v = js.getString();
            if( obj instanceof SolrInputField ) {
              SolrInputField field = (SolrInputField)obj;
              if( "boost".equals( v ) ) {
                ev = js.nextEvent();
                if( ev != JSONParser.NUMBER &&
                    ev != JSONParser.LONG &&  
                    ev != JSONParser.BIGNUMBER ) {
                  throw new IOException( "boost should have number! "+JSONParser.getEventString(ev) );
                }
                field.setBoost( Float.valueOf( js.getNumberChars().toString() ) );
              }
              else if( "value".equals( v  ) ) {
                // nothing special...
                stack.push( field ); // so it can be popped
              }
              else {
                throw new IOException( "invalid key: "+v + " ["+js.getPosition()+"]" );
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
              throw new IOException( "hymmm ["+js.getPosition()+"]" );
            }
          }
          else {
            addValToField(stack, js.getString(), inArray, js);
          }
          break;

        case JSONParser.LONG:
        case JSONParser.NUMBER:
        case JSONParser.BIGNUMBER:
          addValToField(stack, js.getNumberChars().toString(), inArray, js);
          break;
          
        case JSONParser.BOOLEAN:
          addValToField(stack, js.getBoolean(),inArray, js);
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
              throw new IOException( "should not start new object with: "+obj + " ["+js.getPosition()+"]" );
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
            throw new IOException( "should not start new object with: "+obj + " ["+js.getPosition()+"]" );
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

      ev = js.nextEvent();
      if( ev == JSONParser.EOF ) {
        throw new IOException( "should finish doc first!" );
      }
    }
  }
  
  static void addValToField( Stack stack, Object val, boolean inArray, JSONParser js ) throws IOException
  {
    Object obj = stack.peek();
    if( !(obj instanceof SolrInputField) ) {
      throw new IOException( "hymmm ["+js.getPosition()+"]" );
    }
    
    SolrInputField f = inArray
      ? (SolrInputField)obj
      : (SolrInputField)stack.pop();
   
    float boost = (f.getValue()==null)?f.getBoost():1.0f;
    f.addValue( val,boost );
  }


}
