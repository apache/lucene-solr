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

package org.apache.solr.response;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;

import java.io.Writer;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Document;


public final class XMLWriter extends TextResponseWriter {

  public static float CURRENT_VERSION=2.2f;

  private static final char[] XML_START1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".toCharArray();

  private static final char[] XML_STYLESHEET="<?xml-stylesheet type=\"text/xsl\" href=\"/admin/".toCharArray();
  private static final char[] XML_STYLESHEET_END=".xsl\"?>\n".toCharArray();

  /***
  private static final char[] XML_START2_SCHEMA=(
  "<response xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
  +" xsi:noNamespaceSchemaLocation=\"http://pi.cnet.com/cnet-search/response.xsd\">\n"
          ).toCharArray();
  ***/
  
  private static final char[] XML_START2_NOSCHEMA=("<response>\n").toCharArray();

  private boolean defaultIndent=false;
  final int version;

  // temporary working objects...
  // be careful not to use these recursively...
  private final ArrayList tlst = new ArrayList();

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

    String version = req.getParams().get("version");
    float ver = version==null? CURRENT_VERSION : Float.parseFloat(version);
    this.version = (int)(ver*1000);
  }



  public void writeResponse() throws IOException {
    writer.write(XML_START1);

    String stylesheet = req.getParams().get("stylesheet");
    if (stylesheet != null && stylesheet.length() > 0) {
      writer.write(XML_STYLESHEET);
      writer.write(stylesheet);
      writer.write(XML_STYLESHEET_END);
    }

    /***
    String noSchema = req.getParams().get("noSchema");
    // todo - change when schema becomes available?
    if (false && noSchema == null)
      writer.write(XML_START2_SCHEMA);
    else
      writer.write(XML_START2_NOSCHEMA);
     ***/
    writer.write(XML_START2_NOSCHEMA);

    // dump response values
    NamedList lst = rsp.getValues();
    Boolean omitHeader = req.getParams().getBool(CommonParams.OMIT_HEADER);
    if(omitHeader != null && omitHeader) lst.remove("responseHeader");
    int sz = lst.size();
    int start=0;

    // special case the response header if the version is 2.1 or less
    if (version<=2100 && sz>0) {
      Object header = lst.getVal(0);
      if (header instanceof NamedList && "responseHeader".equals(lst.getName(0))) {
        writer.write("<responseHeader>");
        incLevel();
        NamedList nl = (NamedList)header;
        for (int i=0; i<nl.size(); i++) {
          String name = nl.getName(i);
          Object val = nl.getVal(i);
          if ("status".equals(name) || "QTime".equals(name)) {
            writePrim(name,null,val.toString(),false);
          } else {
            writeVal(name,val);
          }
        }
        decLevel();
        writer.write("</responseHeader>");
        start=1;
      }
    }

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
      writeAttr("name", name);
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

  private static final Comparator fieldnameComparator = new Comparator() {
    public int compare(Object o, Object o1) {
      Fieldable f1 = (Fieldable)o; Fieldable f2 = (Fieldable)o1;
      int cmp = f1.name().compareTo(f2.name());
      return cmp;
      // note - the sort is stable, so this should not have affected the ordering
      // of fields with the same name w.r.t eachother.
    }
  };

  @Override
  public final void writeDoc(String name, Document doc, Set<String> returnFields, float score, boolean includeScore) throws IOException {
    startTag("doc", name, false);
    incLevel();

    if (includeScore) {
      writeFloat("score", score);
    }


    // Lucene Documents have multivalued types as multiple fields
    // with the same name.
    // The XML needs to represent these as
    // an array.  The fastest way to detect multiple fields
    // with the same name is to sort them first.


    // using global tlst here, so we shouldn't call any other
    // function that uses it until we are done.
    tlst.clear();
    for (Object obj : doc.getFields()) {
      Fieldable ff = (Fieldable)obj;
      // skip this field if it is not a field to be returned.
      if (returnFields!=null && !returnFields.contains(ff.name())) {
        continue;
      }
      tlst.add(ff);
    }
    Collections.sort(tlst, fieldnameComparator);

    int sz = tlst.size();
    int fidx1 = 0, fidx2 = 0;
    while (fidx1 < sz) {
      Fieldable f1 = (Fieldable)tlst.get(fidx1);
      String fname = f1.name();

      // find the end of fields with this name
      fidx2 = fidx1+1;
      while (fidx2 < sz && fname.equals(((Fieldable)tlst.get(fidx2)).name()) ) {
        fidx2++;
      }

      /***
      // more efficient to use getFieldType instead of
      // getField since that way dynamic fields won't have
      // to create a SchemaField on the fly.
      FieldType ft = schema.getFieldType(fname);
      ***/

      SchemaField sf = schema.getFieldOrNull(fname);
      if( sf == null ) {
        sf = new SchemaField( fname, new TextField() );
      }
      if (fidx1+1 == fidx2) {
        // single field value
        if (version>=2100 && sf.multiValued()) {
          startTag("arr",fname,false);
          doIndent=false;
          sf.write(this, null, f1);
          writer.write("</arr>");
          doIndent=defaultIndent;
        } else {
          sf.write(this, f1.name(), f1);
        }
      } else {
        // multiple fields with same name detected

        startTag("arr",fname,false);
        incLevel();
        doIndent=false;
        int cnt=0;
        for (int i=fidx1; i<fidx2; i++) {
          if (defaultIndent && ++cnt==4) { // only indent every 4th item
            indent();
            cnt=0;
          }
          sf.write(this, null, (Fieldable)tlst.get(i));
        }
        decLevel();
        // if (doIndent) indent();
        writer.write("</arr>");
        // doIndent=true;
        doIndent=defaultIndent;
      }
      fidx1 = fidx2;
    }

    decLevel();
    if (doIndent) indent();
    writer.write("</doc>");
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, Set<String> returnFields, Map pseudoFields) throws IOException {
    startTag("doc", name, false);
    incLevel();

    for (String fname : doc.getFieldNames()) {
      if (returnFields!=null && !returnFields.contains(fname)) {
        continue;
      }
      Object val = doc.getFieldValue(fname);

      if (val instanceof Collection) {
        writeVal(fname, val);
      } else {
        // single valued... figure out if we should put <arr> tags around it anyway
        SchemaField sf = schema.getFieldOrNull(fname);
        if (version>=2100 && sf!=null && sf.multiValued()) {
          startTag("arr",fname,false);
          doIndent=false;
          writeVal(fname, val);
          writer.write("</arr>");
          doIndent=defaultIndent;
        } else {
          writeVal(fname, val);
        }
      }
    }

    if (pseudoFields != null) {
      for (Object fname : pseudoFields.keySet()) {
        writeVal(fname.toString(), pseudoFields.get(fname));
      }
    }

    decLevel();
    if (doIndent) indent();
    writer.write("</doc>");
  }


  private static interface DocumentListInfo {
    Float getMaxScore();
    int getCount();
    long getNumFound();
    long getStart();
    void writeDocs( boolean includeScore, Set<String> fields ) throws IOException;
  }

  private final void writeDocuments(
      String name,
      DocumentListInfo docs,
      Set<String> fields) throws IOException
  {
    boolean includeScore=false;
    if (fields!=null) {
      includeScore = fields.contains("score");
      if (fields.size()==0 || (fields.size()==1 && includeScore) || fields.contains("*")) {
        fields=null;  // null means return all stored fields
      }
    }

    int sz=docs.getCount();
    if (doIndent) indent();

    writer.write("<result");
    writeAttr("name",name);
    writeAttr("numFound",Long.toString(docs.getNumFound()));
    writeAttr("start",Long.toString(docs.getStart()));
    if (includeScore && docs.getMaxScore()!=null) {
      writeAttr("maxScore",Float.toString(docs.getMaxScore()));
    }
    if (sz==0) {
      writer.write("/>");
      return;
    } else {
      writer.write('>');
    }

    incLevel();
    docs.writeDocs(includeScore, fields);
    decLevel();

    if (doIndent) indent();
    writer.write("</result>");
  }

  @Override
  public final void writeSolrDocumentList(String name, final SolrDocumentList docs, Set<String> fields, Map otherFields) throws IOException
  {
    this.writeDocuments( name, new DocumentListInfo()
    {
      public int getCount() {
        return docs.size();
      }

      public Float getMaxScore() {
        return docs.getMaxScore();
      }

      public long getNumFound() {
        return docs.getNumFound();
      }

      public long getStart() {
        return docs.getStart();
      }

      public void writeDocs(boolean includeScore, Set<String> fields) throws IOException {
        for( SolrDocument doc : docs ) {
          writeSolrDocument(null, doc, fields, null);
        }
      }
    }, fields );
  }

  @Override
  public void writeDocList(String name, final DocList ids, Set<String> fields, Map otherFields) throws IOException
  {
    this.writeDocuments( name, new DocumentListInfo()
    {
      public int getCount() {
        return ids.size();
      }

      public Float getMaxScore() {
        return ids.maxScore();
      }

      public long getNumFound() {
        return ids.matches();
      }

      public long getStart() {
        return ids.offset();
      }

      public void writeDocs(boolean includeScore, Set<String> fields) throws IOException {
        SolrIndexSearcher searcher = req.getSearcher();
        DocIterator iterator = ids.iterator();
        int sz = ids.size();
        includeScore = includeScore && ids.hasScores();
        for (int i=0; i<sz; i++) {
          int id = iterator.nextDoc();
          Document doc = searcher.doc(id, fields);
          writeDoc(null, doc, fields, (includeScore ? iterator.score() : 0.0f), includeScore);
        }
      }
    }, fields );
  }


  public void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)

    // go in order of most common to least common
    if (val==null) {
      writeNull(name);
    } else if (val instanceof String) {
      writeStr(name, (String)val, true);
    } else if (val instanceof Integer) {
      // it would be slower to pass the int ((Integer)val).intValue()
      writeInt(name, val.toString());
    } else if (val instanceof Boolean) {
      // could be optimized... only two vals
      writeBool(name, val.toString());
    } else if (val instanceof Long) {
      writeLong(name, val.toString());
    } else if (val instanceof Date) {
      writeDate(name,(Date)val);
    } else if (val instanceof Float) {
      // we pass the float instead of using toString() because
      // it may need special formatting. same for double.
      writeFloat(name, ((Float)val).floatValue());
    } else if (val instanceof Double) {
      writeDouble(name, ((Double)val).doubleValue());
    } else if (val instanceof Document) {
      writeDoc(name, (Document)val, returnFields, 0.0f, false);
    } else if (val instanceof DocList) {
      // requires access to IndexReader
      writeDocList(name, (DocList)val, returnFields, null);
    }else if (val instanceof SolrDocumentList) {
        // requires access to IndexReader
      writeSolrDocumentList(name, (SolrDocumentList)val, returnFields, null);
    }else if (val instanceof DocSet) {
      // how do we know what fields to read?
      // todo: have a DocList/DocSet wrapper that
      // restricts the fields to write...?
    } else if (val instanceof Map) {
      writeMap(name, (Map)val, false, true);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Iterable) {
      writeArray(name,((Iterable)val).iterator());
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else if (val instanceof Iterator) {
      writeArray(name,(Iterator)val);
    } else {
      // default...
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }

  //
  // Generic compound types
  //

  public void writeNamedList(String name, NamedList val) throws IOException {
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
  public void writeMap(String name, Map map, boolean excludeOuter, boolean isFirstVal) throws IOException {
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
  public void writeArray(String name, Iterator iter) throws IOException {
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
