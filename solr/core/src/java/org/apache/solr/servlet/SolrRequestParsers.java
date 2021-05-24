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

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.core.RequestHandlers;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.tracing.GlobalTracer;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.PATH;


public class SolrRequestParsers {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Should these constants be in a more public place?
  public static final String MULTIPART = "multipart";
  public static final String FORMDATA = "formdata";
  public static final String RAW = "raw";
  public static final String SIMPLE = "simple";
  public static final String STANDARD = "standard";
  
  private static final Charset CHARSET_US_ASCII = Charset.forName("US-ASCII");
  
  public static final String INPUT_ENCODING_KEY = "ie";
  private static final byte[] INPUT_ENCODING_BYTES = INPUT_ENCODING_KEY.getBytes(CHARSET_US_ASCII);

  public static final String REQUEST_TIMER_SERVLET_ATTRIBUTE = "org.apache.solr.RequestTimer";

  private final HashMap<String, SolrRequestParser> parsers =
      new HashMap<>();
  private final boolean enableRemoteStreams;
  private final boolean enableStreamBody;
  private StandardRequestParser standard;
  private boolean handleSelect = true;
  private boolean addHttpRequestToContext;

  /** Default instance for e.g. admin requests. Limits to 2 MB uploads and does not allow remote streams. */
  public static final SolrRequestParsers DEFAULT = new SolrRequestParsers();

  /**
   * Pass in an xml configuration.  A null configuration will enable
   * everything with maximum values.
   */
  public SolrRequestParsers( SolrConfig globalConfig ) {
    final int multipartUploadLimitKB, formUploadLimitKB;
    if( globalConfig == null ) {
      multipartUploadLimitKB = formUploadLimitKB = Integer.MAX_VALUE; 
      enableRemoteStreams = false;
      enableStreamBody = false;
      handleSelect = false;
      addHttpRequestToContext = false;
    } else {
      multipartUploadLimitKB = globalConfig.getMultipartUploadLimitKB();
      
      formUploadLimitKB = globalConfig.getFormUploadLimitKB();
      
      enableRemoteStreams = globalConfig.isEnableRemoteStreams();
      enableStreamBody = globalConfig.isEnableStreamBody();
  
      // Let this filter take care of /select?xxx format
      handleSelect = globalConfig.isHandleSelect();
      
      addHttpRequestToContext = globalConfig.isAddHttpRequestToContext();
    }
    init(multipartUploadLimitKB, formUploadLimitKB);
  }
  
  private SolrRequestParsers() {
    enableRemoteStreams = false;
    enableStreamBody = false;
    handleSelect = false;
    addHttpRequestToContext = false;
    init(Integer.MAX_VALUE, Integer.MAX_VALUE);
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

  private static RTimerTree getRequestTimer(HttpServletRequest req)
  {
    final Object reqTimer = req.getAttribute(REQUEST_TIMER_SERVLET_ATTRIBUTE);
    if (reqTimer != null && reqTimer instanceof RTimerTree) {
      return ((RTimerTree) reqTimer);
    }

    return new RTimerTree();
  }

  public SolrQueryRequest parse( SolrCore core, String path, HttpServletRequest req ) throws Exception
  {
    SolrRequestParser parser = standard;
    
    // TODO -- in the future, we could pick a different parser based on the request
    
    // Pick the parser from the request...
    ArrayList<ContentStream> streams = new ArrayList<>(1);
    SolrParams params = parser.parseParamsAndFillStreams( req, streams );
    if (GlobalTracer.get().tracing()) {
      GlobalTracer.getTracer().activeSpan().setTag("params", params.toString());
    }
    SolrQueryRequest sreq = buildRequestFrom(core, params, streams, getRequestTimer(req), req);

    // Handlers and login will want to know the path. If it contains a ':'
    // the handler could use it for RESTful URLs
    sreq.getContext().put(PATH, RequestHandlers.normalize(path));
    sreq.getContext().put("httpMethod", req.getMethod());

    if(addHttpRequestToContext) {
      sreq.getContext().put("httpRequest", req);
    }
    return sreq;
  }

  public SolrQueryRequest buildRequestFrom(SolrCore core, SolrParams params, Collection<ContentStream> streams) throws Exception {
    return buildRequestFrom(core, params, streams, new RTimerTree(), null);
  }

  private SolrQueryRequest buildRequestFrom(SolrCore core, SolrParams params, Collection<ContentStream> streams,
                                            RTimerTree requestTimer, final HttpServletRequest req) throws Exception {
    // The content type will be applied to all streaming content
    String contentType = params.get( CommonParams.STREAM_CONTENTTYPE );
      
    // Handle anything with a remoteURL
    String[] strs = params.getParams( CommonParams.STREAM_URL );
    if( strs != null ) {
      if( !enableRemoteStreams ) {
        throw new SolrException( ErrorCode.BAD_REQUEST, "Remote Streaming is disabled." );
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
        throw new SolrException( ErrorCode.BAD_REQUEST, "Remote Streaming is disabled. See http://lucene.apache.org/solr/guide/requestdispatcher-in-solrconfig.html for help" );
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
      if( !enableStreamBody ) {
        throw new SolrException( ErrorCode.BAD_REQUEST, "Stream Body is disabled. See http://lucene.apache.org/solr/guide/requestdispatcher-in-solrconfig.html for help" );
      }
      for( final String body : strs ) {
        ContentStreamBase stream = new ContentStreamBase.StringStream( body );
        if( contentType != null ) {
          stream.setContentType( contentType );
        }
        streams.add( stream );
      }
    }

    final HttpSolrCall httpSolrCall = req == null ? null : (HttpSolrCall) req.getAttribute(HttpSolrCall.class.getName());
    SolrQueryRequestBase q = new SolrQueryRequestBase(core, params, requestTimer) {
      @Override
      public Principal getUserPrincipal() {
        return req == null ? null : req.getUserPrincipal();
      }

      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        if (httpSolrCall != null) {
          return httpSolrCall.getCommands(validateInput);
        }
        return super.getCommands(validateInput);
      }

      @Override
      public Map<String, String> getPathTemplateValues() {
        if (httpSolrCall != null && httpSolrCall instanceof V2HttpCall) {
          return ((V2HttpCall) httpSolrCall).getUrlParts();
        }
        return super.getPathTemplateValues();
      }

      @Override
      public HttpSolrCall getHttpSolrCall() {
        return httpSolrCall;
      }
    };
    if( streams != null && streams.size() > 0 ) {
      q.setContentStreams( streams );
    }
    return q;
  }

  private static HttpSolrCall getHttpSolrCall(HttpServletRequest req) {
    return req == null ? null : (HttpSolrCall) req.getAttribute(HttpSolrCall.class.getName());
  }
  /**
   * Given a url-encoded query string (UTF-8), map it into solr params
   */
  public static MultiMapSolrParams parseQueryString(String queryString) {
    Map<String,String[]> map = new HashMap<>();
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
                throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: The query string contains a not-%-escaped byte > 127 at position " + pos);
              }
              pos++;
              return ch;
            } else {
              return -1;
            }
          }
        };
        parseFormDataContent(in, Long.MAX_VALUE, StandardCharsets.UTF_8, map, true);
      } catch (IOException ioe) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
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
  @SuppressWarnings({"fallthrough", "resource"})
  static long parseFormDataContent(final InputStream postContent, final long maxLen, Charset charset, final Map<String,String[]> map, boolean supportCharsetParam) throws IOException {
    CharsetDecoder charsetDecoder = supportCharsetParam ? null : getCharsetDecoder(charset);
    final LinkedList<Object> buffer = supportCharsetParam ? new LinkedList<>() : null;
    long len = 0L, keyPos = 0L, valuePos = 0L;
    final ByteArrayOutputStream keyStream = new ByteArrayOutputStream(),
      valueStream = new ByteArrayOutputStream();
    ByteArrayOutputStream currentStream = keyStream;
    for(;;) {
      int b = postContent.read();
      switch (b) {
        case -1: // end of stream
        case '&': // separator
          if (keyStream.size() > 0) {
            final byte[] keyBytes = keyStream.toByteArray(), valueBytes = valueStream.toByteArray();
            if (Arrays.equals(keyBytes, INPUT_ENCODING_BYTES)) {
              // we found a charset declaration in the raw bytes
              if (charsetDecoder != null) {
                throw new SolrException(ErrorCode.BAD_REQUEST,
                  supportCharsetParam ? (
                    "Query string invalid: duplicate '"+
                    INPUT_ENCODING_KEY + "' (input encoding) key."
                  ) : (
                    "Key '" + INPUT_ENCODING_KEY + "' (input encoding) cannot "+
                    "be used in POSTed application/x-www-form-urlencoded form data. "+
                    "To set the input encoding of POSTed form data, use the "+
                    "'Content-Type' header and provide a charset!"
                  )
                );
              }
              // decode the charset from raw bytes
              charset = Charset.forName(decodeChars(valueBytes, keyPos, getCharsetDecoder(CHARSET_US_ASCII)));
              charsetDecoder = getCharsetDecoder(charset);
              // finally decode all buffered tokens
              decodeBuffer(buffer, map, charsetDecoder);
            } else if (charsetDecoder == null) {
              // we have no charset decoder until now, buffer the keys / values for later processing:
              buffer.add(keyBytes);
              buffer.add(Long.valueOf(keyPos));
              buffer.add(valueBytes);
              buffer.add(Long.valueOf(valuePos));
            } else {
              // we already have a charsetDecoder, so we can directly decode without buffering:
              final String key = decodeChars(keyBytes, keyPos, charsetDecoder),
                  value = decodeChars(valueBytes, valuePos, charsetDecoder);
              MultiMapSolrParams.addParam(key.trim(), value, map);
            }
          } else if (valueStream.size() > 0) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded invalid: missing key");
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
        throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content exceeds upload limit of " + (maxLen/1024L) + " KB");
      }
    }
    // if we have not seen a charset declaration, decode the buffer now using the default one (UTF-8 or given via Content-Type):
    if (buffer != null && !buffer.isEmpty()) {
      assert charsetDecoder == null;
      decodeBuffer(buffer, map, getCharsetDecoder(charset));
    }
    return len;
  }
  
  private static CharsetDecoder getCharsetDecoder(Charset charset) {
    return charset.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT);
  }
  
  private static String decodeChars(byte[] bytes, long position, CharsetDecoder charsetDecoder) {
    try {
      return charsetDecoder.decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException cce) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
        "URLDecoder: Invalid character encoding detected after position " + position +
        " of query string / form data (while parsing as " + charsetDecoder.charset().name() + ")"
      );
    }
  }
  
  private static void decodeBuffer(final LinkedList<Object> input, final Map<String,String[]> map, CharsetDecoder charsetDecoder) {
    for (final Iterator<Object> it = input.iterator(); it.hasNext(); ) {
      final byte[] keyBytes = (byte[]) it.next();
      it.remove();
      final Long keyPos = (Long) it.next();
      it.remove();
      final byte[] valueBytes = (byte[]) it.next();
      it.remove();
      final Long valuePos = (Long) it.next();
      it.remove();
      MultiMapSolrParams.addParam(decodeChars(keyBytes, keyPos.longValue(), charsetDecoder).trim(),
          decodeChars(valueBytes, valuePos.longValue(), charsetDecoder), map);
    }
  }
  
  private static int digit16(int b) {
    if (b == -1) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: Incomplete trailing escape (%) pattern");
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
    throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: Invalid digit (" + ((char) b) + ") in escape (%) pattern");
  }
  
  public boolean isHandleSelect() {
    return handleSelect;
  }

  public void setHandleSelect(boolean handleSelect) {
    this.handleSelect = handleSelect;
  }
  
  public boolean isAddRequestHeadersToContext() {
    return addHttpRequestToContext;
  }

  public void setAddRequestHeadersToContext(boolean addRequestHeadersToContext) {
    this.addHttpRequestToContext = addRequestHeadersToContext;
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
  static class SimpleRequestParser implements SolrRequestParser
  {
    @Override
    public SolrParams parseParamsAndFillStreams( 
        final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
    {
      return parseQueryString(req.getQueryString());
    }
  }

  /**
   * Wrap an HttpServletRequest as a ContentStream
   */
  static class HttpRequestContentStream extends ContentStreamBase
  {
    private final InputStream inputStream;

    public HttpRequestContentStream(HttpServletRequest req, InputStream inputStream) {
      this.inputStream = inputStream;

      this.contentType = req.getContentType();
      // name = ???
      // sourceInfo = ???
      
      String v = req.getHeader( "Content-Length" );
      if( v != null ) {
        size = Long.valueOf( v );
      }
    }

    @Override
    public InputStream getStream() throws IOException {
      // we explicitly protect this servlet stream from being closed
      // so that it does not trip our test assert in our close shield
      // in SolrDispatchFilter - we must allow closes from getStream
      // due to the other impls of ContentStream
      return new CloseShieldInputStream(inputStream);
    }
  }

  /**
   * The raw parser just uses the params directly
   */
  static class RawRequestParser implements SolrRequestParser
  {
    @Override
    public SolrParams parseParamsAndFillStreams( 
        final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception
    {
      // If we wrongly add a stream that actually has no content, then it can confuse
      //  some of our code that sees a stream but has no content-type.
      // If we wrongly don't add a stream, then obviously we'll miss data.
      final ServletInputStream inputStream = req.getInputStream(); // don't close it
      if (req.getContentLengthLong() >= 0 || req.getHeader("Transfer-Encoding") != null
            || inputStream.available() > 0) {
        streams.add(new HttpRequestContentStream(req, inputStream));
      } else if (!req.getMethod().equals("GET")) { // GET shouldn't have data
        // We're not 100% sure there is no data, so check by reading a byte (and put back).
        PushbackInputStream pbInputStream = new PushbackInputStream(inputStream);
        int b = pbInputStream.read();
        if (b != -1) {
          pbInputStream.unread(b); // put back
          streams.add(new HttpRequestContentStream(req, pbInputStream));
        }
      }
      return parseQueryString( req.getQueryString() );
    }
  }

  /**
   * Extract Multipart streams
   */
  static class MultipartRequestParser implements SolrRequestParser {
    private final MultipartConfigElement multipartConfigElement;

    public MultipartRequestParser(int uploadLimitKB) {
      multipartConfigElement = new MultipartConfigElement(
          null, // temp dir (null=default)
          -1, // maxFileSize  (-1=none)
          uploadLimitKB * 1024, // maxRequestSize
          100 * 1024 ); // fileSizeThreshold after which will go to disk
    }
    
    @Override
    public SolrParams parseParamsAndFillStreams(
        final HttpServletRequest req, ArrayList<ContentStream> streams) throws Exception {
      if (!isMultipart(req)) {
        throw new SolrException( ErrorCode.BAD_REQUEST, "Not multipart content! "+req.getContentType() );
      }
      // Magic way to tell Jetty dynamically we want multi-part processing.  "Request" here is a Jetty class
      req.setAttribute(Request.MULTIPART_CONFIG_ELEMENT, multipartConfigElement);

      MultiMapSolrParams params = parseQueryString( req.getQueryString() );

      // IMPORTANT: the Parts will all have the delete() method called by cleanupMultipartFiles()

      for (Part part : req.getParts()) {
        if (part.getSubmittedFileName() == null) { // thus a form field and not file upload
          // If it's a form field, put it in our parameter map
          String partAsString = org.apache.commons.io.IOUtils.toString(new PartContentStream(part).getReader());
          MultiMapSolrParams.addParam(
              part.getName().trim(),
              partAsString, params.getMap() );
        } else { // file upload
          streams.add(new PartContentStream(part));
        }
      }
      return params;
    }

    static boolean isMultipart(HttpServletRequest req) {
      // Jetty utilities
      return MimeTypes.Type.MULTIPART_FORM_DATA.is(HttpFields.valueParameters(req.getContentType(), null));
    }

    /** Wrap a MultiPart-{@link Part} as a {@link ContentStream} */
    static class PartContentStream extends ContentStreamBase {
      private final Part part;

      public PartContentStream(Part part ) {
        this.part = part;
        contentType = part.getContentType();
        name = part.getName();
        sourceInfo = part.getSubmittedFileName();
        size = part.getSize();
      }

      @Override
      public InputStream getStream() throws IOException {
        return part.getInputStream();
      }
    }
  }


  /** Clean up any files created by MultiPartInputStream. */
  static void cleanupMultipartFiles(HttpServletRequest request) {
    if (!MultipartRequestParser.isMultipart(request)) {
      return;
    }

    log.debug("Deleting multipart files");

    Collection<Part> parts;
    try {
      parts = request.getParts();
    } catch (Exception e) {
      assert false : e.toString();
      log.error("Couldn't get multipart parts in order to delete them", e);
      return;
    }

    for (Part part : parts) {
      try {
        part.delete();
      } catch (IOException e) {
        log.warn("Errors deleting multipart tmp files", e);
      }
    }
  }

  /**
   * Extract application/x-www-form-urlencoded form data for POST requests
   */
  static class FormDataRequestParser implements SolrRequestParser
  {
    private static final long WS_MASK=(1L<<' ')|(1L<<'\t')|(1L<<'\r')|(1L<<'\n')|(1L<<'#')|(1L<<'/')|(0x01); // set 1 bit so 0xA0 will be flagged as possible whitespace

    private final int uploadLimitKB;
    
    public FormDataRequestParser( int limit )
    {
      uploadLimitKB = limit;
    }

    public SolrParams parseParamsAndFillStreams(HttpServletRequest req, ArrayList<ContentStream> streams, InputStream in) throws Exception {
      final Map<String,String[]> map = new HashMap<>();

      // also add possible URL parameters and include into the map (parsed using UTF-8):
      final String qs = req.getQueryString();
      if (qs != null) {
        parseQueryString(qs, map);
      }

      // may be -1, so we check again later. But if it's already greater we can stop processing!
      final long totalLength = req.getContentLength();
      final long maxLength = ((long) uploadLimitKB) * 1024L;
      if (totalLength > maxLength) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content length (" +
            totalLength + " bytes) exceeds upload limit of " + uploadLimitKB + " KB");
      }

      // get query String from request body, using the charset given in content-type:
      final String cs = ContentStreamBase.getCharsetFromContentType(req.getContentType());
      final Charset charset = (cs == null) ? StandardCharsets.UTF_8 : Charset.forName(cs);

      try {
        // Protect container owned streams from being closed by us, see SOLR-8933
        in = FastInputStream.wrap( in == null ? new CloseShieldInputStream(req.getInputStream()) : in );

        final long bytesRead = parseFormDataContent(in, maxLength, charset, map, false);
        if (bytesRead == 0L && totalLength > 0L) {
          throw getParameterIncompatibilityException();
        }
      } catch (IOException ioe) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
      } catch (IllegalStateException ise) {
        throw (SolrException) getParameterIncompatibilityException().initCause(ise);
      } finally {
        IOUtils.closeWhileHandlingException(in);
      }

      return new MultiMapSolrParams(map);
    }


    @Override
    public SolrParams parseParamsAndFillStreams(HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception {
      if (!isFormData(req)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Not application/x-www-form-urlencoded content: " + req.getContentType());
      }

      return parseParamsAndFillStreams(req, streams, null);
    }

    public static  SolrException getParameterIncompatibilityException() {
      return new SolrException(ErrorCode.SERVER_ERROR,
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
  static class StandardRequestParser implements SolrRequestParser
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
    public SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams ) throws Exception {
      String contentType = req.getContentType();
      String method = req.getMethod(); // No need to uppercase... HTTP verbs are case sensitive
      String uri = req.getRequestURI();
      boolean isV2 = getHttpSolrCall(req) instanceof V2HttpCall;
      boolean isPost = "POST".equals(method);

      // SOLR-6787 changed the behavior of a POST without content type.  Previously it would throw an exception,
      // but now it will use the raw request parser.
      /***
       if (contentType == null && isPost) {
         throw new SolrException(ErrorCode.UNSUPPORTED_MEDIA_TYPE, "Must specify a Content-Type header with POST requests");
       }
       ***/


      // According to previous StandardRequestParser logic (this is a re-written version),
      // POST was handled normally, but other methods (PUT/DELETE)
      // were handled by the RestManager classes if the URI contained /schema or /config
      if (!isPost) {
        if (isV2) {
          return raw.parseParamsAndFillStreams(req, streams);
        }
        if (contentType == null) {
          return parseQueryString(req.getQueryString());
        }

        // OK, we have a BODY at this point

        boolean schemaRestPath = false;
        int idx = uri.indexOf("/schema");
        if (idx >= 0 && uri.endsWith("/schema") || uri.contains("/schema/")) {
          schemaRestPath = true;
        }

        if (schemaRestPath) {
          return raw.parseParamsAndFillStreams(req, streams);
        }

        if ("PUT".equals(method) || "DELETE".equals(method)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unsupported method: " + method + " for request " + req);
        }
      }


      if (formdata.isFormData(req)) {
        String userAgent = req.getHeader("User-Agent");
        boolean isCurl = userAgent != null && userAgent.startsWith("curl/");

        FastInputStream input = FastInputStream.wrap(req.getInputStream());

        if (isCurl) {
          SolrParams params = autodetect(req, streams, input);
          if (params != null) return params;
        }

        return formdata.parseParamsAndFillStreams(req, streams, input);
      }

      if (MultipartRequestParser.isMultipart(req)) {
        return multipart.parseParamsAndFillStreams(req, streams);
      }

      // some other content-type (json, XML, csv, etc)
      return raw.parseParamsAndFillStreams(req, streams);
    }
  }


  private static final long WS_MASK=(1L<<' ')|(1L<<'\t')|(1L<<'\r')|(1L<<'\n')|(1L<<'#')|(1L<<'/')|(0x01); // set 1 bit so 0xA0 will be flagged as possible whitespace

  /** Returns the parameter map if a different content type was auto-detected */
  private static SolrParams autodetect(HttpServletRequest req, ArrayList<ContentStream> streams, FastInputStream in) throws IOException {
    String detectedContentType = null;
    boolean shouldClose = true;

    try {
      in.peek();  // should cause some bytes to be read
      byte[] arr = in.getBuffer();
      int pos = in.getPositionInBuffer();
      int end = in.getEndInBuffer();

      for (int i = pos; i < end - 1; i++) {  // we do "end-1" because we check "arr[i+1]" sometimes in the loop body
        int ch = arr[i];
        boolean isWhitespace = ((WS_MASK >> ch) & 0x01) != 0 && (ch <= ' ' || ch == 0xa0);
        if (!isWhitespace) {
          // first non-whitespace chars
          if (ch == '#'                         // single line comment
              || (ch == '/' && (arr[i + 1] == '/' || arr[i + 1] == '*'))  // single line or multi-line comment
              || (ch == '{' || ch == '[')       // start of JSON object
              )
          {
            detectedContentType = "application/json";
          }
          if (ch == '<') {
            detectedContentType = "text/xml";
          }
          break;
        }
      }

      if (detectedContentType == null) {
        shouldClose = false;
        return null;
      }

      Long size = null;
      String v = req.getHeader("Content-Length");
      if (v != null) {
        size = Long.valueOf(v);
      }
      streams.add(new InputStreamContentStream(in, detectedContentType, size));


      final Map<String, String[]> map = new HashMap<>();
      // also add possible URL parameters and include into the map (parsed using UTF-8):
      final String qs = req.getQueryString();
      if (qs != null) {
        parseQueryString(qs, map);
      }

      return new MultiMapSolrParams(map);

    } catch (IOException ioe) {
      throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
    } catch (IllegalStateException ise) {
      throw (SolrException) FormDataRequestParser.getParameterIncompatibilityException().initCause(ise);
    } finally {
      if (shouldClose) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }


  /**
   * Wrap InputStream as a ContentStream
   */
  static class InputStreamContentStream extends ContentStreamBase {
    private final InputStream is;

    public InputStreamContentStream(InputStream is, String detectedContentType, Long size ) {
      this.is = is;
      this.contentType = detectedContentType;
      this.size = size;
    }

    @Override
    public InputStream getStream() throws IOException {
      return is;
    }
  }



}
