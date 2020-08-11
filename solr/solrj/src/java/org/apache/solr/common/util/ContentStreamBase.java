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
package org.apache.solr.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import org.apache.http.entity.ContentType;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;

/**
 * Three concrete implementations for ContentStream - one for File/URL/String
 * 
 *
 * @since solr 1.2
 */
public abstract class ContentStreamBase implements ContentStream
{

  public static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();
  private static final String TEXT_CSV = "text/csv";
  private static final List<String> UNHELPFUL_TYPES = Arrays.asList(ContentType.APPLICATION_OCTET_STREAM.getMimeType(), "application/gzip", "content/unknown");
  private static final List<String> XML_SUF =  Arrays.asList(".xml", ".xml.gz", ".xml.gzip");
  private static final List<String> JSON_SUF =  Arrays.asList(".json", ".json.gz", ".json.gzip");
  private static final List<String> CSV_SUF =  Arrays.asList(".csv", ".csv.gz", ".csv.gzip");

  protected String name;
  protected String sourceInfo;
  protected String contentType;
  protected Long size;
  
  //---------------------------------------------------------------------
  //---------------------------------------------------------------------
  
  public static String getCharsetFromContentType( String contentType )
  {
    if( contentType != null ) {
      int idx = contentType.toLowerCase(Locale.ROOT).indexOf( "charset=" );
      if( idx > 0 ) {
        return contentType.substring( idx + "charset=".length() ).trim();
      }
    }
    return null;
  }

  protected String attemptToDetermineContentType() {
    String type = null;
    if (name != null) {
      Predicate<String> endsWith = suffix->name.toLowerCase(Locale.ROOT).endsWith(suffix);

      if (XML_SUF.stream().anyMatch(endsWith)) {
        type = ContentType.APPLICATION_XML.getMimeType();
      } else if (JSON_SUF.stream().anyMatch(endsWith)) {
        type = ContentType.APPLICATION_JSON.getMimeType();
      } else if (CSV_SUF.stream().anyMatch(endsWith)) {
        type = TEXT_CSV;
      } else {
        type = attemptToDetermineTypeFromFirstCharacter();
      }
    }
    return type;
  }

  private String attemptToDetermineTypeFromFirstCharacter() {
    String type = null;
    try (InputStream stream = getStream()) {
      // Last ditch effort to determine content, if the first non-white space
      // is a '<' or '{', assume xml or json.
      int data = stream.read();
      while (( data != -1 ) && ( ( (char)data ) == ' ' )) {
        data = stream.read();
      }
      if ((char)data == '<') {
        type = ContentType.APPLICATION_XML.getMimeType();
      } else if ((char)data == '{') {
        type = ContentType.APPLICATION_JSON.getMimeType();
      }
    } catch (Exception ex) {
      // This code just eats, the exception and leaves
      // the contentType untouched.
    }
    return type;
  }

  //------------------------------------------------------------------------
  //------------------------------------------------------------------------
  
  /**
   * Construct a <code>ContentStream</code> from a <code>URL</code>
   * 
   * This uses a <code>URLConnection</code> to get the content stream
   * @see  URLConnection
   */
  public static class URLStream extends ContentStreamBase
  {
    private final URL url;
    
    public URLStream( URL url ) {
      this.url = url; 
      sourceInfo = "url";
    }

    @Override
    public String getContentType() {
      // for file:// streams that are octet-streams, try to determine the payload
      // type from payload rather than just using the mime type.
      if ("file".equals(url.getProtocol())) {
        Predicate<String> equals = mimeType->mimeType.equals(contentType);
        if (UNHELPFUL_TYPES.stream().anyMatch(equals)) {
          String type = attemptToDetermineContentType();
          contentType = ( type != null ) ? type : contentType;
        }
      }
      return contentType;
    }

    @Override
    public InputStream getStream() throws IOException {
      URLConnection conn = this.url.openConnection();
      
      contentType = conn.getContentType();
      name = url.toExternalForm();
      size = conn.getContentLengthLong();
      InputStream is = conn.getInputStream();
      String urlFile = url.getFile().toLowerCase(Locale.ROOT);
      if( "gzip".equals(conn.getContentEncoding()) || urlFile.endsWith( ".gz" ) || urlFile.endsWith( ".gzip" )){
        is = new GZIPInputStream(is);
      }
      return is;
    }
  }
  
  /**
   * Construct a <code>ContentStream</code> from a <code>File</code>
   */
  public static class FileStream extends ContentStreamBase
  {
    private final File file;
    
    public FileStream( File f ) {
      file = f; 
      
      contentType = null; // ??
      name = file.getName();
      size = file.length();
      sourceInfo = file.toURI().toString();
    }

    @Override
    public String getContentType() {
      if(contentType==null) {
        contentType = attemptToDetermineContentType();
      }
      return contentType;
    }

    @Override
    public InputStream getStream() throws IOException {
      InputStream is = new FileInputStream( file );
      String lowerName = name.toLowerCase(Locale.ROOT);
      if(lowerName.endsWith(".gz") || lowerName.endsWith(".gzip")) {
        is = new GZIPInputStream(is);
      }
      return is;
    }
  }
  

  /**
   * Construct a <code>ContentStream</code> from a <code>String</code>
   */
  public static class StringStream extends ContentStreamBase
  {
    private final String str;

    public StringStream( String str ) {
      this(str, detect(str));
    }

    public StringStream( String str, String contentType ) {
      this.str = str;
      this.contentType = contentType;
      name = null;
      try {
        size = (long) str.getBytes(DEFAULT_CHARSET).length;
      } catch (UnsupportedEncodingException e) {
        // won't happen
        throw new RuntimeException(e);
      }
      sourceInfo = "string";
    }

    public static String detect(String str) {
      String detectedContentType = null;
      int lim = str.length() - 1;
      for (int i=0; i<lim; i++) {
        char ch = str.charAt(i);
        if (Character.isWhitespace(ch)) {
          continue;
        }
        // first non-whitespace chars
        if (ch == '#'                         // single line comment
            || (ch == '/' && (str.charAt(i + 1) == '/' || str.charAt(i + 1) == '*'))  // single line or multi-line comment
            || (ch == '{' || ch == '[')       // start of JSON object
            )
        {
          detectedContentType = "application/json";
        } else if (ch == '<') {
          detectedContentType = "text/xml";
        }
        break;
      }
      return detectedContentType;
    }

    @Override
    public InputStream getStream() throws IOException {
      return new ByteArrayInputStream( str.getBytes(DEFAULT_CHARSET) );
    }

    /**
     * If an charset is defined (by the contentType) use that, otherwise 
     * use a StringReader
     */
    @Override
    public Reader getReader() throws IOException {
      String charset = getCharsetFromContentType( contentType );
      return charset == null 
        ? new StringReader( str )
        : new InputStreamReader( getStream(), charset );
    }
  }

  /**
   * Base reader implementation.  If the contentType declares a 
   * charset use it, otherwise use "utf-8".
   */
  @Override
  public Reader getReader() throws IOException {
    String charset = getCharsetFromContentType( getContentType() );
    return charset == null 
      ? new InputStreamReader( getStream(), DEFAULT_CHARSET )
      : new InputStreamReader( getStream(), charset );
  }

  //------------------------------------------------------------------
  // Getters / Setters for overrideable attributes
  //------------------------------------------------------------------

  @Override
  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public Long getSize() {
    return size;
  }

  public void setSize(Long size) {
    this.size = size;
  }

  @Override
  public String getSourceInfo() {
    return sourceInfo;
  }

  public void setSourceInfo(String sourceInfo) {
    this.sourceInfo = sourceInfo;
  }
  public static ContentStream create(RequestWriter requestWriter,
                                     @SuppressWarnings({"rawtypes"})SolrRequest req) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(req);
    contentWriter.write(baos);
    return new ByteArrayStream(baos.toByteArray(), null,contentWriter.getContentType() );
  }
  
  /**
   * Construct a <code>ContentStream</code> from a <code>File</code>
   */
  public static class ByteArrayStream extends ContentStreamBase
  {
    private final byte[] bytes;
    public ByteArrayStream( byte[] bytes, String source ) {
      this(bytes,source, null);
    }
    
    public ByteArrayStream( byte[] bytes, String source, String contentType ) {
      this.bytes = bytes;
      
      this.contentType = contentType;
      name = source;
      size = (long) bytes.length;
      sourceInfo = source;
    }


    @Override
    public InputStream getStream() throws IOException {
      return new ByteArrayInputStream( bytes );
    }
  }  
}
