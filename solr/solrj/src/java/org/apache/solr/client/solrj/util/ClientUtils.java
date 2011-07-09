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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.nio.ByteBuffer;

import org.apache.commons.httpclient.util.DateParseException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.*;


/**
 * TODO? should this go in common?
 * 
 *
 * @since solr 1.3
 */
public class ClientUtils 
{
  // Standard Content types
  public static final String TEXT_XML = "application/xml; charset=UTF-8";  
  
  /**
   * Take a string and make it an iterable ContentStream
   */
  public static Collection<ContentStream> toContentStreams( final String str, final String contentType )
  {
    if( str == null )
      return null;

    ArrayList<ContentStream> streams = new ArrayList<ContentStream>( 1 );
    ContentStreamBase ccc = new ContentStreamBase.StringStream( str );
    ccc.setContentType( contentType );
    streams.add( ccc );
    return streams;
  }

  /**
   * @param d SolrDocument to convert
   * @return a SolrInputDocument with the same fields and values as the
   *   SolrDocument.  All boosts are 1.0f
   */
  public static SolrInputDocument toSolrInputDocument( SolrDocument d )
  {
    SolrInputDocument doc = new SolrInputDocument();
    for( String name : d.getFieldNames() ) {
      doc.addField( name, d.getFieldValue(name), 1.0f );
    }
    return doc;
  }

  /**
   * @param d SolrInputDocument to convert
   * @return a SolrDocument with the same fields and values as the SolrInputDocument
   */
  public static SolrDocument toSolrDocument( SolrInputDocument d )
  {
    SolrDocument doc = new SolrDocument();
    for( SolrInputField field : d ) {
      doc.setField( field.getName(), field.getValue() );
    }
    return doc;
  }

  //------------------------------------------------------------------------
  //------------------------------------------------------------------------

  public static void writeXML( SolrInputDocument doc, Writer writer ) throws IOException
  {
    writer.write("<doc boost=\""+doc.getDocumentBoost()+"\">");

    for( SolrInputField field : doc ) {
      float boost = field.getBoost();
      String name = field.getName();
      for( Object v : field ) {
        if (v instanceof Date) {
          v = DateUtil.getThreadLocalDateFormat().format( (Date)v );
        }else if (v instanceof byte[]) {
          byte[] bytes = (byte[]) v;
          v = Base64.byteArrayToBase64(bytes, 0,bytes.length);
        } else if (v instanceof ByteBuffer) {
          ByteBuffer bytes = (ByteBuffer) v;
          v = Base64.byteArrayToBase64(bytes.array(), bytes.position(),bytes.limit() - bytes.position());
        }

        if( boost != 1.0f ) {
          XML.writeXML(writer, "field", v.toString(), "name", name, "boost", boost );
        } else if (v != null) {
          XML.writeXML(writer, "field", v.toString(), "name", name );
        }

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

  /**
   * @deprecated Use {@link org.apache.solr.common.util.DateUtil#DEFAULT_DATE_FORMATS}
   */
  @Deprecated
  public static final Collection<String> fmts = DateUtil.DEFAULT_DATE_FORMATS;

  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   * @throws ParseException
   * @throws DateParseException
   *
   * @deprecated Use {@link org.apache.solr.common.util.DateUtil#parseDate(String)}
   */
  @Deprecated
  public static Date parseDate( String d ) throws ParseException, DateParseException
  {
    return DateUtil.parseDate(d);
  }

  /**
   * Returns a formatter that can be use by the current thread if needed to
   * convert Date objects to the Internal representation.
   *
   * @deprecated use {@link org.apache.solr.common.util.DateUtil#getThreadLocalDateFormat()}
   */
  @Deprecated
  public static DateFormat getThreadLocalDateFormat() {

    return DateUtil.getThreadLocalDateFormat();
  }

  /**
   * @deprecated Use {@link org.apache.solr.common.util.DateUtil#UTC}.
   */
  @Deprecated
  public static TimeZone UTC = DateUtil.UTC;



  /**
   * See: <a href="http://lucene.apache.org/java/docs/nightly/queryparsersyntax.html#Escaping%20Special%20Characters">Escaping Special Characters</a>
   */
  public static String escapeQueryChars(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!'  || c == '(' || c == ')' || c == ':'
        || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
        || c == '*' || c == '?' || c == '|' || c == '&'  || c == ';'
        || Character.isWhitespace(c)) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
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
