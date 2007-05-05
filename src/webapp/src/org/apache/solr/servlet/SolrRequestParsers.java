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

package org.apache.solr.servlet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.util.ContentStream;
import org.apache.solr.request.MultiMapSolrParams;
import org.apache.solr.request.ServletSolrParams;
import org.apache.solr.request.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.util.ContentStreamBase;


public class SolrRequestParsers 
{
  final Logger log = Logger.getLogger(SolrRequestParsers.class.getName());
  
  // Should these constants be in a more public place?
  public static final String MULTIPART = "multipart";
  public static final String RAW = "raw";
  public static final String SIMPLE = "simple";
  public static final String STANDARD = "standard";
  
  private HashMap<String, SolrRequestParser> parsers;
  private SolrCore core;
  private boolean enableRemoteStreams = false;
  private StandardRequestParser standard;
  
  public SolrRequestParsers( SolrCore core, Config config )
  {
    this.core = core;
    
    // Read the configuration
    long uploadLimitKB = SolrConfig.config.getInt( 
        "requestDispatcher/requestParsers/@multipartUploadLimitInKB", 2000 ); // 2MB default
    
    this.enableRemoteStreams = SolrConfig.config.getBool( 
        "requestDispatcher/requestParsers/@enableRemoteStreaming", false ); 
        
    MultipartRequestParser multi = new MultipartRequestParser( uploadLimitKB );
    RawRequestParser raw = new RawRequestParser();
    standard = new StandardRequestParser( multi, raw );
    
    // I don't see a need to have this publically configured just yet
    // adding it is trivial
    parsers = new HashMap<String, SolrRequestParser>();
    parsers.put( MULTIPART, multi );
    parsers.put( RAW, raw );
    parsers.put( SIMPLE, new SimpleRequestParser() );
    parsers.put( STANDARD, standard );
    parsers.put( "", standard );
  }
  
  public SolrQueryRequest parse( String path, HttpServletRequest req ) throws Exception
  {
    SolrRequestParser parser = standard;
    
    // TODO -- in the future, we could pick a different parser based on the request
    
    // Pick the parer from the request...
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>(1);
    SolrParams params = parser.parseParamsAndFillStreams( req, streams );
    SolrQueryRequest sreq = buildRequestFrom( params, streams );

    // Handlers and loggin will want to know the path. If it contains a ':' 
    // the handler could use it for RESTfull URLs
    sreq.getContext().put( "path", path );
    return sreq;
  }
  
  SolrQueryRequest buildRequestFrom( SolrParams params, List<ContentStream> streams ) throws Exception
  {
    // The content type will be applied to all streaming content
    String contentType = params.get( SolrParams.STREAM_CONTENTTYPE );
      
    // Handle anything with a remoteURL
    String[] strs = params.getParams( SolrParams.STREAM_URL );
    if( strs != null ) {
      if( !enableRemoteStreams ) {
        throw new SolrException( 400, "Remote Streaming is disabled." );
      }
      for( final String url : strs ) {
        ContentStreamBase stream = new ContentStreamBase.URLStream( new URL(url) );
        if( contentType != null ) {
          stream.setContentType( contentType );
        }
        streams.add( stream );
      }
    }
    
    // Handle streaming files
    strs = params.getParams( SolrParams.STREAM_FILE );
    if( strs != null ) {
      if( !enableRemoteStreams ) {
        throw new SolrException( 400, "Remote Streaming is disabled." );
      }
      for( final String file : strs ) {
        ContentStreamBase stream = new ContentStreamBase.FileStream( new File(file) );
        if( contentType != null ) {
          stream.setContentType( contentType );
        }
        streams.add( stream );
      }
    }
    
    // Check for streams in the request parameters
    strs = params.getParams( SolrParams.STREAM_BODY );
    if( strs != null ) {
      for( final String body : strs ) {
        ContentStreamBase stream = new ContentStreamBase.StringStream( body );
        if( contentType != null ) {
          stream.setContentType( contentType );
        }
        streams.add( stream );
      }
    }
    
    SolrQueryRequestBase q = new SolrQueryRequestBase( core, params ) { };
    if( streams != null && streams.size() > 0 ) {
      q.setContentStreams( streams );
    }
    return q;
  }
  

  /**
   * Given a standard query string map it into solr params
   */
  public static MultiMapSolrParams parseQueryString(String queryString) 
  {
    Map<String,String[]> map = new HashMap<String, String[]>();
    if( queryString != null && queryString.length() > 0 ) {
      try {
        for( String kv : queryString.split( "&" ) ) {
          int idx = kv.indexOf( '=' );
          if( idx > 0 ) {
            String name = URLDecoder.decode( kv.substring( 0, idx ), "UTF-8");
            String value = URLDecoder.decode( kv.substring( idx+1 ), "UTF-8");
            MultiMapSolrParams.addParam( name, value, map );
          }
          else {
            String name = URLDecoder.decode( kv, "UTF-8" );
            MultiMapSolrParams.addParam( name, "", map );
          }
        }
      }
      catch( UnsupportedEncodingException uex ) {
        throw new SolrException( 500, uex );
      }
    }
    return new MultiMapSolrParams( map );
  }
}

//-----------------------------------------------------------------
//-----------------------------------------------------------------

// I guess we don't really even need the interface, but i'll keep it here just for kicks
interface SolrRequestParser 
{
  public SolrParams parseParamsAndFillStreams(
    final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception;
}


//-----------------------------------------------------------------
//-----------------------------------------------------------------

/**
 * The simple parser just uses the params directly
 */
class SimpleRequestParser implements SolrRequestParser
{
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    return new ServletSolrParams(req);
  }
}


/**
 * The simple parser just uses the params directly
 */
class RawRequestParser implements SolrRequestParser
{
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    streams.add( new ContentStream() {
      public String getContentType() {
        return req.getContentType();
      }
      public String getName() {
        return null; // Is there any meaningfull name?
      }
      public String getSourceInfo() {
        return null; // Is there any meaningfull name?
      }
      public Long getSize() { 
        String v = req.getHeader( "Content-Length" );
        if( v != null ) {
          return Long.valueOf( v );
        }
        return null; 
      }
      public InputStream getStream() throws IOException {
        return req.getInputStream();
      }
      public Reader getReader() throws IOException {
        return req.getReader();
      }
    });
    return SolrRequestParsers.parseQueryString( req.getQueryString() );
  }
}



/**
 * Extract Multipart streams
 */
class MultipartRequestParser implements SolrRequestParser
{
  private long uploadLimitKB;
  
  public MultipartRequestParser( long limit )
  {
    uploadLimitKB = limit;
  }
  
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    if( !ServletFileUpload.isMultipartContent(req) ) {
      throw new SolrException( 400, "Not multipart content! "+req.getContentType() );
    }
    
    MultiMapSolrParams params = SolrRequestParsers.parseQueryString( req.getQueryString() );
    
    // Create a factory for disk-based file items
    DiskFileItemFactory factory = new DiskFileItemFactory();

    // Set factory constraints
    // TODO - configure factory.setSizeThreshold(yourMaxMemorySize);
    // TODO - configure factory.setRepository(yourTempDirectory);

    // Create a new file upload handler
    ServletFileUpload upload = new ServletFileUpload(factory);
    upload.setSizeMax( uploadLimitKB*1024 );

    // Parse the request
    List items = upload.parseRequest(req);
    Iterator iter = items.iterator();
    while (iter.hasNext()) {
        FileItem item = (FileItem) iter.next();

        // If its a form field, put it in our parameter map
        if (item.isFormField()) {
          MultiMapSolrParams.addParam( 
            item.getFieldName(), 
            item.getString(), params.getMap() );
        }
        // Only add it if it actually has something...
        else if( item.getSize() > 0 ) { 
          streams.add( new FileItemContentStream( item ) );
        }
    }
    return params;
  }
  
  /**
   * Wrap a FileItem as a ContentStream
   */
  private static class FileItemContentStream extends ContentStreamBase
  {
    FileItem item;
    
    public FileItemContentStream( FileItem f )
    {
      item = f;
    }
    
    public String getContentType() {
      return item.getContentType();
    }
    
    public String getName() {
      return item.getName();
    }
    
    public InputStream getStream() throws IOException {
      return item.getInputStream();
    }

    public String getSourceInfo() {
      return item.getFieldName();
    }
    
    public Long getSize()
    {
      return item.getSize();
    }
  }
}


/**
 * The default Logic
 */
class StandardRequestParser implements SolrRequestParser
{
  MultipartRequestParser multipart;
  RawRequestParser raw;
  
  StandardRequestParser( MultipartRequestParser multi, RawRequestParser raw ) 
  {
    this.multipart = multi;
    this.raw = raw;
  }
  
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    String method = req.getMethod().toUpperCase();
    if( "GET".equals( method ) ) {
      return new ServletSolrParams(req);
    }
    if( "POST".equals( method ) ) {
      String contentType = req.getContentType();
      if( contentType != null ) {
        int idx = contentType.indexOf( ';' );
        if( idx > 0 ) { // remove the charset definition "; charset=utf-8"
          contentType = contentType.substring( 0, idx );
        }
        if( "application/x-www-form-urlencoded".equals( contentType.toLowerCase() ) ) {
          return new ServletSolrParams(req); // just get the params from parameterMap
        }
        if( ServletFileUpload.isMultipartContent(req) ) {
          return multipart.parseParamsAndFillStreams(req, streams);
        }
      }
      return raw.parseParamsAndFillStreams(req, streams);
    }
    throw new SolrException( 400, "Unsuported method: "+method );
  }
}







