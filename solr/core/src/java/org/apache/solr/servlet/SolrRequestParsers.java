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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.ServletSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;


public class SolrRequestParsers 
{
  final Logger log = LoggerFactory.getLogger(SolrRequestParsers.class);
  
  // Should these constants be in a more public place?
  public static final String MULTIPART = "multipart";
  public static final String RAW = "raw";
  public static final String SIMPLE = "simple";
  public static final String STANDARD = "standard";
  
  private HashMap<String, SolrRequestParser> parsers;
  private boolean enableRemoteStreams = false;
  private boolean handleSelect = true;
  private StandardRequestParser standard;
  
  /**
   * Pass in an xml configuration.  A null configuration will enable
   * everythign with maximum values.
   */
  public SolrRequestParsers( Config globalConfig )
  {
    long uploadLimitKB = 1048;  // 2MB default
    if( globalConfig == null ) {
      uploadLimitKB = Long.MAX_VALUE; 
      enableRemoteStreams = true;
      handleSelect = true;
    }
    else {
      uploadLimitKB = globalConfig.getInt( 
          "requestDispatcher/requestParsers/@multipartUploadLimitInKB", (int)uploadLimitKB );
      
      enableRemoteStreams = globalConfig.getBool( 
          "requestDispatcher/requestParsers/@enableRemoteStreaming", false ); 
  
      // Let this filter take care of /select?xxx format
      handleSelect = globalConfig.getBool( 
          "requestDispatcher/@handleSelect", handleSelect ); 
    }
       
    MultipartRequestParser multi = new MultipartRequestParser( uploadLimitKB );
    RawRequestParser raw = new RawRequestParser();
    standard = new StandardRequestParser( multi, raw );
    
    // I don't see a need to have this publicly configured just yet
    // adding it is trivial
    parsers = new HashMap<String, SolrRequestParser>();
    parsers.put( MULTIPART, multi );
    parsers.put( RAW, raw );
    parsers.put( SIMPLE, new SimpleRequestParser() );
    parsers.put( STANDARD, standard );
    parsers.put( "", standard );
  }
  
  public SolrQueryRequest parse( SolrCore core, String path, HttpServletRequest req ) throws Exception
  {
    SolrRequestParser parser = standard;
    
    // TODO -- in the future, we could pick a different parser based on the request
    
    // Pick the parser from the request...
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>(1);
    SolrParams params = parser.parseParamsAndFillStreams( req, streams );
    SolrQueryRequest sreq = buildRequestFrom( core, params, streams );

    // Handlers and login will want to know the path. If it contains a ':'
    // the handler could use it for RESTful URLs
    sreq.getContext().put( "path", path );
    return sreq;
  }
  
  public SolrQueryRequest buildRequestFrom( SolrCore core, SolrParams params, Collection<ContentStream> streams ) throws Exception
  {
    // The content type will be applied to all streaming content
    String contentType = params.get( CommonParams.STREAM_CONTENTTYPE );
      
    // Handle anything with a remoteURL
    String[] strs = params.getParams( CommonParams.STREAM_URL );
    if( strs != null ) {
      if( !enableRemoteStreams ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Remote Streaming is disabled." );
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
    strs = params.getParams( CommonParams.STREAM_FILE );
    if( strs != null ) {
      if( !enableRemoteStreams ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Remote Streaming is disabled." );
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
    strs = params.getParams( CommonParams.STREAM_BODY );
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
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, uex );
      }
    }
    return new MultiMapSolrParams( map );
  }

  public boolean isHandleSelect() {
    return handleSelect;
  }

  public void setHandleSelect(boolean handleSelect) {
    this.handleSelect = handleSelect;
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
 * Wrap an HttpServletRequest as a ContentStream
 */
class HttpRequestContentStream extends ContentStreamBase
{
  private final HttpServletRequest req;
  
  public HttpRequestContentStream( HttpServletRequest req ) throws IOException {
    this.req = req;
    
    contentType = req.getContentType();
    // name = ???
    // sourceInfo = ???
    
    String v = req.getHeader( "Content-Length" );
    if( v != null ) {
      size = Long.valueOf( v );
    }
  }

  public InputStream getStream() throws IOException {
    return req.getInputStream();
  }
}


/**
 * Wrap a FileItem as a ContentStream
 */
class FileItemContentStream extends ContentStreamBase
{
  private final FileItem item;
  
  public FileItemContentStream( FileItem f )
  {
    item = f;
    contentType = item.getContentType();
    name = item.getName();
    sourceInfo = item.getFieldName();
    size = item.getSize();
  }
    
  public InputStream getStream() throws IOException {
    return item.getInputStream();
  }
}

/**
 * The raw parser just uses the params directly
 */
class RawRequestParser implements SolrRequestParser
{
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    // The javadocs for HttpServletRequest are clear that req.getReader() should take
    // care of any character encoding issues.  BUT, there are problems while running on
    // some servlet containers: including Tomcat 5 and resin.
    //
    // Rather than return req.getReader(), this uses the default ContentStreamBase method
    // that checks for charset definitions in the ContentType.
    
    streams.add( new HttpRequestContentStream( req ) );
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
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Not multipart content! "+req.getContentType() );
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
        // Add the stream
        else { 
          streams.add( new FileItemContentStream( item ) );
        }
    }
    return params;
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
    String method = req.getMethod().toUpperCase(Locale.ENGLISH);
    if( "GET".equals( method ) || "HEAD".equals( method )) {
      return new ServletSolrParams(req);
    }
    if( "POST".equals( method ) ) {
      String contentType = req.getContentType();
      if( contentType != null ) {
        int idx = contentType.indexOf( ';' );
        if( idx > 0 ) { // remove the charset definition "; charset=utf-8"
          contentType = contentType.substring( 0, idx );
        }
        if( "application/x-www-form-urlencoded".equals( contentType.toLowerCase(Locale.ENGLISH) ) ) {
          return new ServletSolrParams(req); // just get the params from parameterMap
        }
        if( ServletFileUpload.isMultipartContent(req) ) {
          return multipart.parseParamsAndFillStreams(req, streams);
        }
      }
      return raw.parseParamsAndFillStreams(req, streams);
    }
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unsupported method: "+method );
  }
}









