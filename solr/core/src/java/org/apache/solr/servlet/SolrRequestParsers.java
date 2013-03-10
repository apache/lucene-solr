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

package org.apache.solr.servlet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.net.URL;
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
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;


public class SolrRequestParsers 
{
  final Logger log = LoggerFactory.getLogger(SolrRequestParsers.class);
  
  // Should these constants be in a more public place?
  public static final String MULTIPART = "multipart";
  public static final String FORMDATA = "formdata";
  public static final String RAW = "raw";
  public static final String SIMPLE = "simple";
  public static final String STANDARD = "standard";
  
  private final HashMap<String, SolrRequestParser> parsers =
      new HashMap<String, SolrRequestParser>();
  private final boolean enableRemoteStreams;
  private StandardRequestParser standard;
  private boolean handleSelect = true;
  
  /** Default instance for e.g. admin requests. Limits to 2 MB uploads and does not allow remote streams. */
  public static final SolrRequestParsers DEFAULT = new SolrRequestParsers();
  
  /**
   * Pass in an xml configuration.  A null configuration will enable
   * everything with maximum values.
   */
  public SolrRequestParsers( Config globalConfig ) {
    final int multipartUploadLimitKB, formUploadLimitKB;
    if( globalConfig == null ) {
      multipartUploadLimitKB = formUploadLimitKB = Integer.MAX_VALUE; 
      enableRemoteStreams = true;
      handleSelect = true;
    } else {
      multipartUploadLimitKB = globalConfig.getInt( 
          "requestDispatcher/requestParsers/@multipartUploadLimitInKB", 2048 );
      
      formUploadLimitKB = globalConfig.getInt( 
          "requestDispatcher/requestParsers/@formdataUploadLimitInKB", 2048 );
      
      enableRemoteStreams = globalConfig.getBool( 
          "requestDispatcher/requestParsers/@enableRemoteStreaming", false ); 
  
      // Let this filter take care of /select?xxx format
      handleSelect = globalConfig.getBool( 
          "requestDispatcher/@handleSelect", true ); 
    }
    init(multipartUploadLimitKB, formUploadLimitKB);
  }
  
  private SolrRequestParsers() {
    enableRemoteStreams = false;
    handleSelect = false;
    init(2048, 2048);
  }

  private void init( int multipartUploadLimitKB, int formUploadLimitKB) {       
    MultipartRequestParser multi = new MultipartRequestParser( multipartUploadLimitKB );
    RawRequestParser raw = new RawRequestParser();
    FormDataRequestParser formdata = new FormDataRequestParser( formUploadLimitKB );
    standard = new StandardRequestParser( multi, raw, formdata );
    
    // I don't see a need to have this publicly configured just yet
    // adding it is trivial
    parsers.put( MULTIPART, multi );
    parsers.put( FORMDATA, formdata );
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
   * Given a url-encoded query string (UTF-8), map it into solr params
   */
  public static MultiMapSolrParams parseQueryString(String queryString) {
    Map<String,String[]> map = new HashMap<String, String[]>();
    parseQueryString(queryString, map);
    return new MultiMapSolrParams(map);
  }

  /**
   * Given a url-encoded query string (UTF-8), map it into the given map
   * @param queryString as given from URL
   * @param map place all parameters in this map
   */
  static void parseQueryString(final String queryString, final Map<String,String[]> map) {
    if (queryString != null && queryString.length() > 0) {
      try {
        final int len = queryString.length();
        // this input stream emulates to get the raw bytes from the URL as passed to servlet container, it disallows any byte > 127 and enforces to %-escape them:
        final InputStream in = new InputStream() {
          int pos = 0;
          @Override
          public int read() {
            if (pos < len) {
              final char ch = queryString.charAt(pos);
              if (ch > 127) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "URLDecoder: The query string contains a not-%-escaped byte > 127 at position " + pos);
              }
              pos++;
              return ch;
            } else {
              return -1;
            }
          }
        };
        parseFormDataContent(in, Long.MAX_VALUE, IOUtils.CHARSET_UTF_8, map);
      } catch (IOException ioe) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ioe);
      }
    }
  }
  
  /**
   * Given a url-encoded form from POST content (as InputStream), map it into the given map.
   * The given InputStream should be buffered!
   * @param postContent to be parsed
   * @param charset to be used to decode resulting bytes after %-decoding
   * @param map place all parameters in this map
   */
  @SuppressWarnings("fallthrough")
  static long parseFormDataContent(final InputStream postContent, final long maxLen, final Charset charset, final Map<String,String[]> map) throws IOException {
    final CharsetDecoder charsetDecoder = charset.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT);
    long len = 0L, keyPos = 0L, valuePos = 0L;
    final ByteArrayOutputStream2 keyStream = new ByteArrayOutputStream2(),
      valueStream = new ByteArrayOutputStream2();
    ByteArrayOutputStream2 currentStream = keyStream;
    for(;;) {
      int b = postContent.read();
      switch (b) {
        case -1: // end of stream
        case '&': // separator
          if (keyStream.size() > 0) {
            final String key = decodeChars(keyStream, keyPos, charsetDecoder), value = decodeChars(valueStream, valuePos, charsetDecoder);
            MultiMapSolrParams.addParam(key, value, map);
          } else if (valueStream.size() > 0) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded invalid: missing key");
          }
          keyStream.reset();
          valueStream.reset();
          keyPos = valuePos = len + 1;
          currentStream = keyStream;
          break;
        case '+': // space replacement
          currentStream.write(' ');
          break;
        case '%': // escape
          final int upper = digit16(b = postContent.read());
          len++;
          final int lower = digit16(b = postContent.read());
          len++;
          currentStream.write(((upper << 4) + lower));
          break;
        case '=': // kv separator
          if (currentStream == keyStream) {
            valuePos = len + 1;
            currentStream = valueStream;
            break;
          }
          // fall-through
        default:
          currentStream.write(b);
      }
      if (b == -1) {
        break;
      }
      len++;
      if (len > maxLen) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content exceeds upload limit of " + (maxLen/1024L) + " KB");
      }
    }
    return len;
  }
  
  private static String decodeChars(ByteArrayOutputStream2 stream, long position, CharsetDecoder charsetDecoder) {
    try {
      return charsetDecoder.decode(ByteBuffer.wrap(stream.buffer(), 0, stream.size())).toString();
    } catch (CharacterCodingException cce) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "URLDecoder: Invalid character encoding detected after position " + position +
        " of query string / form data (while parsing as " + charsetDecoder.charset().name() + ")"
      );
    }
  }
  
  /** Makes the buffer of ByteArrayOutputStream available without copy. */
  static final class ByteArrayOutputStream2 extends ByteArrayOutputStream {
    byte[] buffer() {
      return buf;
    }
  }
  
  private static int digit16(int b) {
    if (b == -1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "URLDecoder: Incomplete trailing escape (%) pattern");
    }
    if (b >= '0' && b <= '9') {
      return b - '0';
    }
    if (b >= 'A' && b <= 'F') {
      return b - ('A' - 10);
    }
    if (b >= 'a' && b <= 'f') {
      return b - ('a' - 10);
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "URLDecoder: Invalid digit (" + ((char) b) + ") in escape (%) pattern");
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
 * The simple parser just uses the params directly, does not support POST URL-encoded forms
 */
class SimpleRequestParser implements SolrRequestParser
{
  @Override
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    return SolrRequestParsers.parseQueryString(req.getQueryString());
  }
}

/**
 * Wrap an HttpServletRequest as a ContentStream
 */
class HttpRequestContentStream extends ContentStreamBase
{
  private final HttpServletRequest req;
  
  public HttpRequestContentStream( HttpServletRequest req ) {
    this.req = req;
    
    contentType = req.getContentType();
    // name = ???
    // sourceInfo = ???
    
    String v = req.getHeader( "Content-Length" );
    if( v != null ) {
      size = Long.valueOf( v );
    }
  }

  @Override
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
    
  @Override
  public InputStream getStream() throws IOException {
    return item.getInputStream();
  }
}

/**
 * The raw parser just uses the params directly
 */
class RawRequestParser implements SolrRequestParser
{
  @Override
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    streams.add( new HttpRequestContentStream( req ) );
    return SolrRequestParsers.parseQueryString( req.getQueryString() );
  }
}



/**
 * Extract Multipart streams
 */
class MultipartRequestParser implements SolrRequestParser
{
  private final int uploadLimitKB;
  
  public MultipartRequestParser( int limit )
  {
    uploadLimitKB = limit;
  }
  
  @Override
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
    upload.setSizeMax( ((long) uploadLimitKB) * 1024L );

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
 * Extract application/x-www-form-urlencoded form data for POST requests
 */
class FormDataRequestParser implements SolrRequestParser
{
  private final int uploadLimitKB;
  
  public FormDataRequestParser( int limit )
  {
    uploadLimitKB = limit;
  }
  
  @Override
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    if (!isFormData(req)) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Not application/x-www-form-urlencoded content: "+req.getContentType() );
    }
    
    final Map<String,String[]> map = new HashMap<String, String[]>();
    
    // also add possible URL parameters and include into the map (parsed using UTF-8):
    final String qs = req.getQueryString();
    if (qs != null) {
      SolrRequestParsers.parseQueryString(qs, map);
    }
    
    // may be -1, so we check again later. But if its already greater we can stop processing!
    final long totalLength = req.getContentLength();
    final long maxLength = ((long) uploadLimitKB) * 1024L;
    if (totalLength > maxLength) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content length (" +
        totalLength + " bytes) exceeds upload limit of " + uploadLimitKB + " KB");
    }
    
    // get query String from request body, using the charset given in content-type:
    final String cs = ContentStreamBase.getCharsetFromContentType(req.getContentType());
    final Charset charset = (cs == null) ? IOUtils.CHARSET_UTF_8 : Charset.forName(cs);
    InputStream in = null;
    try {
      in = req.getInputStream();
      final long bytesRead = SolrRequestParsers.parseFormDataContent(FastInputStream.wrap(in), maxLength, charset, map);
      if (bytesRead == 0L && totalLength > 0L) {
        throw getParameterIncompatibilityException();
      }
    } catch (IOException ioe) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ioe);
    } catch (IllegalStateException ise) {
      throw (SolrException) getParameterIncompatibilityException().initCause(ise);
    } finally {
      IOUtils.closeWhileHandlingException(in);
    }
    
    return new MultiMapSolrParams(map);
  }
  
  private SolrException getParameterIncompatibilityException() {
    return new SolrException(SolrException.ErrorCode.SERVER_ERROR,
      "Solr requires that request parameters sent using application/x-www-form-urlencoded " +
      "content-type can be read through the request input stream. Unfortunately, the " +
      "stream was empty / not available. This may be caused by another servlet filter calling " +
      "ServletRequest.getParameter*() before SolrDispatchFilter, please remove it."
    );
  }
  
  public boolean isFormData(HttpServletRequest req) {
    String contentType = req.getContentType();
    if (contentType != null) {
      int idx = contentType.indexOf( ';' );
      if( idx > 0 ) { // remove the charset definition "; charset=utf-8"
        contentType = contentType.substring( 0, idx );
      }
      contentType = contentType.trim();
      if( "application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
        return true;
      }
    }
    return false;
  }
}


/**
 * The default Logic
 */
class StandardRequestParser implements SolrRequestParser
{
  MultipartRequestParser multipart;
  RawRequestParser raw;
  FormDataRequestParser formdata;
  
  StandardRequestParser(MultipartRequestParser multi, RawRequestParser raw, FormDataRequestParser formdata) 
  {
    this.multipart = multi;
    this.raw = raw;
    this.formdata = formdata;
  }
  
  @Override
  public SolrParams parseParamsAndFillStreams( 
      final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
  {
    String method = req.getMethod().toUpperCase(Locale.ROOT);
    if ("GET".equals(method) || "HEAD".equals(method)) {
      return SolrRequestParsers.parseQueryString(req.getQueryString());
    }
    if ("POST".equals( method ) ) {
      if (formdata.isFormData(req)) {
        return formdata.parseParamsAndFillStreams(req, streams);
      }
      if (ServletFileUpload.isMultipartContent(req)) {
        return multipart.parseParamsAndFillStreams(req, streams);
      }
      return raw.parseParamsAndFillStreams(req, streams);
    }
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unsupported method: "+method );
  }
}









