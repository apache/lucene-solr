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

package org.apache.solr.client.solrj.util;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.XML;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;


/**
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

    ArrayList<ContentStream> streams = new ArrayList<>( 1 );
    ContentStreamBase ccc = new ContentStreamBase.StringStream( str );
    ccc.setContentType( contentType );
    streams.add( ccc );
    return streams;
  }

  /**
   * @param d SolrDocument to convert
   * @return a SolrInputDocument with the same fields and values as the
   *   SolrDocument.  All boosts are 1.0f
   * @deprecated This method will be removed in Solr 6.0
   */
  @Deprecated
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
   * @deprecated This method will be removed in Solr 6.0
   */
  @Deprecated
  public static SolrDocument toSolrDocument(SolrInputDocument d) {
    SolrDocument doc = new SolrDocument();
    for (SolrInputField field : d) {
      doc.setField(field.getName(), field.getValue());
    }
    if (d.getChildDocuments() != null) {
      for (SolrInputDocument in : d.getChildDocuments()) {
        doc.addChildDocument(toSolrDocument(in));
      }

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
        String update = null;

        if (v instanceof Map) {
          // currently only supports a single value
          for (Entry<Object,Object> entry : ((Map<Object,Object>)v).entrySet()) {
            update = entry.getKey().toString();
            v = entry.getValue();
            if (v instanceof Collection) {
              Collection values = (Collection) v;
              for (Object value : values) {
                writeVal(writer, boost, name, value, update);
                boost = 1.0f;
              }
            } else  {
              writeVal(writer, boost, name, v, update);
              boost = 1.0f;
            }
          }
        } else  {
          writeVal(writer, boost, name, v, update);
          // only write the boost for the first multi-valued field
          // otherwise, the used boost is the product of all the boost values
          boost = 1.0f;
        }
      }
    }

    if (doc.hasChildDocuments()) {
      for (SolrInputDocument childDocument : doc.getChildDocuments()) {
        writeXML(childDocument, writer);
      }
    }
    
    writer.write("</doc>");
  }

  private static void writeVal(Writer writer, float boost, String name, Object v, String update) throws IOException {
    if (v instanceof Date) {
      v = DateUtil.getThreadLocalDateFormat().format( (Date)v );
    } else if (v instanceof byte[]) {
      byte[] bytes = (byte[]) v;
      v = Base64.byteArrayToBase64(bytes, 0, bytes.length);
    } else if (v instanceof ByteBuffer) {
      ByteBuffer bytes = (ByteBuffer) v;
      v = Base64.byteArrayToBase64(bytes.array(), bytes.position(),bytes.limit() - bytes.position());
    }

    if (update == null) {
      if( boost != 1.0f ) {
        XML.writeXML(writer, "field", v.toString(), "name", name, "boost", boost);
      } else if (v != null) {
        XML.writeXML(writer, "field", v.toString(), "name", name );
      }
    } else {
      if( boost != 1.0f ) {
        XML.writeXML(writer, "field", v.toString(), "name", name, "boost", boost, "update", update);
      } else {
        if (v == null)  {
          XML.writeXML(writer, "field", null, "name", name, "update", update, "null", true);
        } else  {
          XML.writeXML(writer, "field", v.toString(), "name", name, "update", update);
        }
      }
    }
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
   * See: <a href="https://www.google.com/?gws_rd=ssl#q=lucene+query+parser+syntax">Lucene query parser syntax</a>
   * for more information on Escaping Special Characters
   */
  // NOTE: its broken to link to any lucene-queryparser.jar docs, not in classpath!!!!!
  public static String escapeQueryChars(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!'  || c == '(' || c == ')' || c == ':'
        || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
        || c == '*' || c == '?' || c == '|' || c == '&'  || c == ';' || c == '/'
        || Character.isWhitespace(c)) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /** Constructs a slices map from a collection of slices and handles disambiguation if multiple collections are being queried simultaneously */
  public static void addSlices(Map<String,Slice> target, String collectionName, Collection<Slice> slices, boolean multiCollection) {
    for (Slice slice : slices) {
      String key = slice.getName();
      if (multiCollection) key = collectionName + "_" + key;
      target.put(key, slice);
    }
  }
}
