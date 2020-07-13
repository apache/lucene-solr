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
package org.apache.solr.response;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;

import static org.apache.solr.common.params.CommonParams.NAME;


/**
 * @lucene.internal
 */
public class XMLWriter extends TextResponseWriter {

  public static float CURRENT_VERSION=2.2f;

  private static final char[] XML_START1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".toCharArray();

  private static final char[] XML_STYLESHEET="<?xml-stylesheet type=\"text/xsl\" href=\"".toCharArray();
  private static final char[] XML_STYLESHEET_END="\"?>\n".toCharArray();

  /*
  private static final char[] XML_START2_SCHEMA=(
  "<response xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
  +" xsi:noNamespaceSchemaLocation=\"http://pi.cnet.com/cnet-search/response.xsd\">\n"
          ).toCharArray();
  ***/

  private static final char[] XML_START2_NOSCHEMA=("<response>\n").toCharArray();

  final int version;

  public static void writeResponse(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    XMLWriter xmlWriter = null;
    try {
      xmlWriter = new XMLWriter(writer, req, rsp);
      xmlWriter.writeResponse();
    } finally {
      xmlWriter.close();
    }
  }

  public XMLWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);

    String version = req.getParams().get(CommonParams.VERSION);
    float ver = version==null? CURRENT_VERSION : Float.parseFloat(version);
    this.version = (int)(ver*1000);
    if( this.version < 2200 ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "XMLWriter does not support version: "+version );
    }
  }



  public void writeResponse() throws IOException {
    writer.write(XML_START1);

    String stylesheet = req.getParams().get("stylesheet");
    if (stylesheet != null && stylesheet.length() > 0) {
      writer.write(XML_STYLESHEET);
      XML.escapeAttributeValue(stylesheet, writer);
      writer.write(XML_STYLESHEET_END);
    }

    /*
    String noSchema = req.getParams().get("noSchema");
    // todo - change when schema becomes available?
    if (false && noSchema == null)
      writer.write(XML_START2_SCHEMA);
    else
      writer.write(XML_START2_NOSCHEMA);
     ***/
    writer.write(XML_START2_NOSCHEMA);

    // dump response values
    Boolean omitHeader = req.getParams().getBool(CommonParams.OMIT_HEADER);
    if(omitHeader != null && omitHeader) rsp.removeResponseHeader();
    final NamedList<?> lst = rsp.getValues();
    int sz = lst.size();
    int start=0;

    for (int i=start; i<sz; i++) {
      writeVal(lst.getName(i),lst.getVal(i));
    }

    writer.write("\n</response>\n");
  }





  /** Writes the XML attribute name/val. A null val means that the attribute is missing. */
  private void writeAttr(String name, String val) throws IOException {
    writeAttr(name, val, true);
  }

  public void writeAttr(String name, String val, boolean escape) throws IOException{
    if (val != null) {
      writer.write(' ');
      writer.write(name);
      writer.write("=\"");
      if(escape){
        XML.escapeAttributeValue(val, writer);
      } else {
        writer.write(val);
      }
      writer.write('"');
    }
  }

  void startTag(String tag, String name, boolean closeTag) throws IOException {
    if (doIndent) indent();

    writer.write('<');
    writer.write(tag);
    if (name!=null) {
      writeAttr(NAME, name);
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write(">");
      }
    } else {
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write('>');
      }
    }
  }


  @Override
  public void writeStartDocumentList(String name,
      long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException
  {
    if (doIndent) indent();

    writer.write("<result");
    writeAttr(NAME, name);
    writeAttr("numFound",Long.toString(numFound));
    writeAttr("start",Long.toString(start));
    if (maxScore != null) {
      writeAttr("maxScore",Float.toString(maxScore));
    }
    if (numFoundExact != null) {
      writeAttr("numFoundExact", numFoundExact.toString());
    }
    writer.write(">");

    incLevel();
  }
  
  @Override
  @Deprecated
  public void writeStartDocumentList(String name,
      long start, int size, long numFound, Float maxScore) throws IOException
  {
    if (doIndent) indent();

    writer.write("<result");
    writeAttr(NAME, name);
    writeAttr("numFound",Long.toString(numFound));
    writeAttr("start",Long.toString(start));
    if(maxScore!=null) {
      writeAttr("maxScore",Float.toString(maxScore));
    }
    writer.write(">");

    incLevel();
  }


  /**
   * The SolrDocument should already have multivalued fields implemented as
   * Collections -- this will not rewrite to &lt;arr&gt;
   */
  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx ) throws IOException {
    startTag("doc", name, false);
    incLevel();

    for (String fname : doc.getFieldNames()) {
      if (returnFields!= null && !returnFields.wantsField(fname)) {
        continue;
      }

      Object val = doc.getFieldValue(fname);
      if( "_explain_".equals( fname ) ) {
        System.out.println( val );
        }
      writeVal(fname, val);
    }

    if(doc.hasChildDocuments()) {
      for(SolrDocument childDoc : doc.getChildDocuments()) {
        writeSolrDocument(null, childDoc, new SolrReturnFields(), idx);
      }
    }

    decLevel();
    writer.write("</doc>");
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    decLevel();
    if (doIndent) indent();
    writer.write("</result>");
  }



  //
  // Generic compound types
  //

  @Override
  public void writeNamedList(String name, @SuppressWarnings({"rawtypes"})NamedList val) throws IOException {
    int sz = val.size();
    startTag("lst", name, sz<=0);

    incLevel();
    for (int i=0; i<sz; i++) {
      writeVal(val.getName(i),val.getVal(i));
    }
    decLevel();

    if (sz > 0) {
      if (doIndent) indent();
      writer.write("</lst>");
    }
  }

  @Override
  public void writeMap(String name, MapWriter val) throws IOException {
    // As the size is not known. So, always both startTag and endTag is written
    // irrespective of number of entries in MapWriter
    startTag("lst", name, false);
    incLevel();

    val.writeMap(new MapWriter.EntryWriter() {
      @Override
      public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
        writeVal( null == k ? null : k.toString(), v);
        return this;
      }
    });

    decLevel();
    if (doIndent) {
      indent();
    }
    writer.write("</lst>");
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void writeMap(String name, @SuppressWarnings({"rawtypes"})Map map, boolean excludeOuter, boolean isFirstVal) throws IOException {
    int sz = map.size();

    if (!excludeOuter) {
      startTag("lst", name, sz<=0);
      incLevel();
    }

    for (Map.Entry entry : (Set<Map.Entry>)map.entrySet()) {
      Object k = entry.getKey();
      Object v = entry.getValue();
      // if (sz<indentThreshold) indent();
      writeVal( null == k ? null : k.toString(), v);
    }

    if (!excludeOuter) {
      decLevel();
      if (sz > 0) {
        if (doIndent) indent();
        writer.write("</lst>");
      }
    }
  }

  @Override
  public void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val).iterator());
  }

  @Override
  public void writeArray(String name, @SuppressWarnings({"rawtypes"})Iterator iter) throws IOException {
    if( iter.hasNext() ) {
      startTag("arr", name, false );
      incLevel();
      while( iter.hasNext() ) {
        writeVal(null, iter.next());
      }
      decLevel();
      if (doIndent) indent();
      writer.write("</arr>");
    }
    else {
      startTag("arr", name, true );
    }
  }

  @Override
  public void writeIterator(String name, IteratorWriter val) throws IOException {
    // As the size is not known. So, always both startTag and endTag is written
    // irrespective of number of entries in IteratorWriter
    startTag("arr", name, false );
    incLevel();

    val.writeIter(new IteratorWriter.ItemWriter() {
      @Override
      public IteratorWriter.ItemWriter add(Object o) throws IOException {
        writeVal(null, o);
        return this;
      }
    });

    decLevel();
    if (doIndent) {
      indent();
    }
    writer.write("</arr>");
  }

  //
  // Primitive types
  //

  @Override
  public void writeNull(String name) throws IOException {
    writePrim("null",name,"",false);
  }

  @Override
  public void writeStr(String name, String val, boolean escape) throws IOException {
    writePrim("str",name,val,escape);
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    writePrim("int",name,val,false);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    writePrim("long",name,val,false);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    writePrim("bool",name,val,false);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    writePrim("float",name,val,false);
  }

  @Override
  public void writeFloat(String name, float val) throws IOException {
    writeFloat(name,Float.toString(val));
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    writePrim("double",name,val,false);
  }

  @Override
  public void writeDouble(String name, double val) throws IOException {
    writeDouble(name,Double.toString(val));
  }


  @Override
  public void writeDate(String name, String val) throws IOException {
    writePrim("date",name,val,false);
  }


  //
  // OPT - specific writeInt, writeFloat, methods might be faster since
  // there would be less write calls (write("<int name=\"" + name + ... + </int>)
  //
  private void writePrim(String tag, String name, String val, boolean escape) throws IOException {
    int contentLen = val==null ? 0 : val.length();

    startTag(tag, name, contentLen==0);
    if (contentLen==0) return;

    if (escape) {
      XML.escapeCharData(val,writer);
    } else {
      writer.write(val,0,contentLen);
    }

    writer.write('<');
    writer.write('/');
    writer.write(tag);
    writer.write('>');
  }

}
