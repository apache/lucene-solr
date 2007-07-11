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

package org.apache.solr.client.solrj.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.XML;


/**
 * TODO? should this go in common?
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class ClientUtils 
{
  // Standard Content types
  public static final String TEXT_XML = "text/xml; charset=utf-8";  
  
  /**
   * Take a string and make it an iterable ContentStream
   */
  public static Collection<ContentStream> toContentStreams( final String str, final String contentType )
  {
    ContentStreamBase ccc = new ContentStreamBase.StringStream( str );
    ccc.setContentType( contentType );
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
    streams.add( ccc );
    return streams;
  }
  
  //------------------------------------------------------------------------
  //------------------------------------------------------------------------

  private static void writeFieldValue(Writer writer, String fieldName, Float boost, Object fieldValue) throws IOException 
  {
    if (fieldValue instanceof Date) {
      fieldValue = fmtThreadLocal.get().format( (Date)fieldValue );
    }
    if( boost != null ) {
      XML.writeXML(writer, "field", fieldValue.toString(), "name", fieldName, "boost", boost );          
    }
    else if( fieldValue != null ){
      XML.writeXML(writer, "field", fieldValue.toString(), "name", fieldName);
    }
  }
  
  public static void writeXML( SolrInputDocument doc, Writer writer ) throws IOException
  {
    writer.write("<doc boost=\""+doc.getDocumentBoost()+"\">");
   
    for( SolrInputField field : doc ) {
      float boost = field.getBoost();
      for( Object o : field ) {
        writeFieldValue(writer, field.getName(), boost, o );
        // only write the boost for the first multi-valued field
        // otherwise, the used boost is the product of all the boost values
        boost = 1.0f; 
      }
    }
    writer.write("</doc>");
  }
  

  public static String toXML( SolrInputDocument doc ) 
  {
    StringWriter str = new StringWriter();
    try {
      writeXML( doc, str );
    }
    catch( Exception ex ){}
    return str.toString();
  }
  
  //---------------------------------------------------------------------------------------

  public static final Collection<String> fmts = new ArrayList<String>();
  static {
    fmts.add( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
    fmts.add( "yyyy-MM-dd'T'HH:mm:ss" );
    fmts.add( "yyyy-MM-dd" );
  }
  
  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   * @throws ParseException 
   * @throws DateParseException 
   */
  public static Date parseDate( String d ) throws ParseException, DateParseException 
  {
    // 2007-04-26T08:05:04Z
    if( d.endsWith( "Z" ) && d.length() > 20 ) {
      return getThreadLocalDateFormat().parse( d );
    }
    return DateUtil.parseDate( d, fmts ); 
  }
  
  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   */
  public static DateFormat getThreadLocalDateFormat() {
  
    return fmtThreadLocal.get();
  }

  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static ThreadLocalDateFormat fmtThreadLocal = new ThreadLocalDateFormat();
  
  private static class ThreadLocalDateFormat extends ThreadLocal<DateFormat> {
    DateFormat proto;
    public ThreadLocalDateFormat() {
      super();
                                    //2007-04-26T08:05:04Z
      SimpleDateFormat tmp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      tmp.setTimeZone(UTC);
      proto = tmp;
    }
    
    @Override
    protected DateFormat initialValue() {
      return (DateFormat) proto.clone();
    }
  }
  

  /**
   * See: http://lucene.apache.org/java/docs/queryparsersyntax.html#Escaping Special Characters
   */
  public static String escapeQueryChars( String input ) 
  {
    char buff[] = input.toCharArray();
    StringBuilder str = new StringBuilder( buff.length+5 );
    for( char c : buff ) {
      switch( c ) {
      case '+':
      case '-':
      case '&':
      case '|':
      case '(':
      case ')':
      case '{':
      case '}':
      case '[':
      case ']':
      case '^':
      case '"':
      case '*':
      case ':':
      case '\\':
        str.append( '\\' );
      }
      str.append( c );
    }
    return str.toString();
  }
  

  public static String toQueryString( SolrParams params, boolean xml ) {
    StringBuilder sb = new StringBuilder(128);
    try {
      String amp = xml ? "&amp;" : "&";
      boolean first=true;
      Iterator<String> names = params.getParameterNamesIterator();
      while( names.hasNext() ) {
        String key = names.next();
        String[] valarr = params.getParams( key );
        if( valarr == null ) {
          sb.append( first?"?":amp );
          sb.append(key);
          first=false;
        }
        else {
          for (String val : valarr) {
            sb.append( first? "?":amp );
            sb.append(key);
            if( val != null ) {
              sb.append('=');
              sb.append( URLEncoder.encode( val, "UTF-8" ) );
            }
            first=false;
          }
        }
      }
    }
    catch (IOException e) {throw new RuntimeException(e);}  // can't happen
    return sb.toString();
  }
}
