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

package org.apache.solr.request;

import org.apache.solr.util.NamedList;
import org.apache.solr.util.XML;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import java.io.Writer;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Document;
/**
 * @author yonik
 * @version $Id$
 */
final public class XMLWriter {

  public static float CURRENT_VERSION=2.2f;

  //
  // static thread safe part
  //
  private static final char[] XML_START1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".toCharArray();

  private static final char[] XML_STYLESHEET="<?xml-stylesheet type=\"text/xsl\" href=\"/admin/".toCharArray();
  private static final char[] XML_STYLESHEET_END=".xsl\"?>\n".toCharArray();

  private static final char[] XML_START2_SCHEMA=(
  "<response xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
  +" xsi:noNamespaceSchemaLocation=\"http://pi.cnet.com/cnet-search/response.xsd\">\n"
          ).toCharArray();
  private static final char[] XML_START2_NOSCHEMA=(
  "<response>\n"
          ).toCharArray();


  public static void writeResponse(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {

    String ver = req.getParam("version");

    writer.write(XML_START1);

    String stylesheet = req.getParam("stylesheet");
    if (stylesheet != null && stylesheet.length() > 0) {
      writer.write(XML_STYLESHEET);
      writer.write(stylesheet);
      writer.write(XML_STYLESHEET_END);
    }

    String noSchema = req.getParam("noSchema");
    // todo - change when schema becomes available?
    if (false && noSchema == null)
      writer.write(XML_START2_SCHEMA);
    else
      writer.write(XML_START2_NOSCHEMA);

    // create an instance for each request to handle
    // non-thread safe stuff (indentation levels, etc)
    // and to encapsulate writer, schema, and searcher so
    // they don't have to be passed around in every function.
    //
    XMLWriter xw = new XMLWriter(writer, req.getSchema(), req.getSearcher(), ver);
    xw.defaultFieldList = rsp.getReturnFields();

    String indent = req.getParam("indent");
    if (indent != null) {
      if ("".equals(indent) || "off".equals(indent)) {
        xw.setIndent(false);
      } else {
        xw.setIndent(true);
      }
    }

    // dump response values
    NamedList lst = rsp.getValues();
    int sz = lst.size();
    int start=0;

    // special case the response header if the version is 2.1 or less    
    if (xw.version<=2100 && sz>0) {
      Object header = lst.getVal(0);
      if (header instanceof NamedList && "responseHeader".equals(lst.getName(0))) {
        writer.write("<responseHeader>");
        xw.incLevel();
        NamedList nl = (NamedList)header;
        for (int i=0; i<nl.size(); i++) {
          String name = nl.getName(i);
          Object val = nl.getVal(i);
          if ("status".equals(name) || "QTime".equals(name)) {
            xw.writePrim(name,null,val.toString(),false);
          } else {
            xw.writeVal(name,val);
          }
        }
        xw.decLevel();
        writer.write("</responseHeader>");
        start=1;
      }
    }

    for (int i=start; i<sz; i++) {
      xw.writeVal(lst.getName(i),lst.getVal(i));
    }

    writer.write("\n</response>\n");
  }


  ////////////////////////////////////////////////////////////
  // request instance specific (non-static, not shared between threads)
  ////////////////////////////////////////////////////////////

  private final Writer writer;
  private final IndexSchema schema; // needed to write fields of docs
  private final SolrIndexSearcher searcher;  // needed to retrieve docs

  private int level;
  private boolean defaultIndent=false;
  private boolean doIndent=false;

  // fieldList... the set of fields to return for each document
  private Set<String> defaultFieldList;


  // if a list smaller than this threshold is encountered, elements
  // will be written on the same line.
  // maybe constructed types should always indent first?
  private final int indentThreshold=0;

  final int version;


  // temporary working objects...
  // be careful not to use these recursively...
  private final ArrayList tlst = new ArrayList();
  private final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
  private final StringBuilder sb = new StringBuilder();

  public XMLWriter(Writer writer, IndexSchema schema, SolrIndexSearcher searcher, String version) {
    this.writer = writer;
    this.schema = schema;
    this.searcher = searcher;
    float ver = version==null? CURRENT_VERSION : Float.parseFloat(version);
    this.version = (int)(ver*1000);
  }

  //
  // Functions to manipulate the current logical nesting level.
  // Any indentation will be partially based on level.
  //
  public void setLevel(int level) { this.level = level; }
  public int level() { return level; }
  public int incLevel() { return ++level; }
  public int decLevel() { return --level; }
  public void setIndent(boolean doIndent) {
    this.doIndent = doIndent;
    defaultIndent = doIndent;
  }

  public void writeAttr(String name, String val) throws IOException {
    if (val != null) {
      writer.write(' ');
      writer.write(name);
      writer.write("=\"");
      XML.escapeAttributeValue(val, writer);
      writer.write('"');
    }
  }

  public void startTag(String tag, String name, boolean closeTag) throws IOException {
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

  private static final String[] indentArr = new String[] {
    "\n",
    "\n ",
    "\n  ",
    "\n\t",
    "\n\t ",
    "\n\t  ",  // could skip this one (the only 3 char seq)
    "\n\t\t" };

  public void indent() throws IOException {
     indent(level);
  }

  public void indent(int lev) throws IOException {
    int arrsz = indentArr.length-1;
    // another option would be lev % arrsz (wrap around)
    String istr = indentArr[ lev > arrsz ? arrsz : lev ];
    writer.write(istr);
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

      SchemaField sf = schema.getField(fname);

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

  public final void writeDocList(String name, DocList ids, Set<String> fields) throws IOException {
    boolean includeScore=false;
    if (fields!=null) {
      includeScore = fields.contains("score");
      if (fields.size()==0 || (fields.size()==1 && includeScore) || fields.contains("*")) {
        fields=null;  // null means return all stored fields
      }
    }

    int sz=ids.size();

    if (doIndent) indent();
    writer.write("<result");
    writeAttr("name",name);
    writeAttr("numFound",Integer.toString(ids.matches()));
    writeAttr("start",Integer.toString(ids.offset()));
    if (includeScore) {
      writeAttr("maxScore",Float.toString(ids.maxScore()));
    }
    if (sz==0) {
      writer.write("/>");
      return;
    } else {
      writer.write('>');
    }

    incLevel();
    DocIterator iterator = ids.iterator();
    for (int i=0; i<sz; i++) {
      int id = iterator.nextDoc();
      Document doc = searcher.doc(id, fields);
      writeDoc(null, doc, fields, (includeScore ? iterator.score() : 0.0f), includeScore);
    }
    decLevel();

    if (doIndent) indent();
    writer.write("</result>");
  }


  public void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)

    // go in order of most common to least common
    if (val==null) {
      writeNull(name);
    } else if (val instanceof String) {
      writeStr(name, (String)val);
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
      writeDoc(name, (Document)val, defaultFieldList, 0.0f, false);
    } else if (val instanceof DocList) {
      // requires access to IndexReader
      writeDocList(name, (DocList)val, defaultFieldList);
    } else if (val instanceof DocSet) {
      // how do we know what fields to read?
      // todo: have a DocList/DocSet wrapper that
      // restricts the fields to write...?
    } else if (val instanceof Map) {
      writeMap(name, (Map)val);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Collection) {
      writeArray(name,(Collection)val);
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else {
      // default...
      writeStr(name, val.getClass().getName() + ':' + val.toString());
    }
  }

  //
  // Generic compound types
  //

  public void writeNamedList(String name, NamedList val) throws IOException {
    int sz = val.size();
    startTag("lst", name, sz<=0);

    if (sz<indentThreshold) {
      doIndent=false;
    }

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



  //A map is currently represented as a named list
  public void writeMap(String name, Map val) throws IOException {
    Map map = val;
    int sz = map.size();
    startTag("lst", name, sz<=0);
    incLevel();
    for (Map.Entry entry : (Set<Map.Entry>)map.entrySet()) {
      // possible class-cast exception here...
      String k = (String)entry.getKey();
      Object v = entry.getValue();
      // if (sz<indentThreshold) indent();
      writeVal(k,v);
    }
    decLevel();
    if (sz > 0) {
      if (doIndent) indent();
      writer.write("</lst>");
    }
  }

  public void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val));
  }

  public void writeArray(String name, Collection val) throws IOException {
    int sz = val.size();
    startTag("arr", name, sz<=0);
    incLevel();
    for (Object o : val) {
      // if (sz<indentThreshold) indent();
      writeVal(null, o);
    }
    decLevel();
    if (sz > 0) {
      if (doIndent) indent();
      writer.write("</arr>");
    }
  }

  //
  // Primitive types
  //

  public void writeNull(String name) throws IOException {
    writePrim("null",name,"",false);
  }

  public void writeStr(String name, String val) throws IOException {
    writePrim("str",name,val,true);
  }

  public void writeInt(String name, String val) throws IOException {
    writePrim("int",name,val,false);
  }

  public void writeInt(String name, int val) throws IOException {
    writeInt(name,Integer.toString(val));
  }

  public void writeLong(String name, String val) throws IOException {
    writePrim("long",name,val,false);
  }

  public void writeLong(String name, long val) throws IOException {
    writeLong(name,Long.toString(val));
  }

  public void writeBool(String name, String val) throws IOException {
    writePrim("bool",name,val,false);
  }

  public void writeBool(String name, boolean val) throws IOException {
    writeBool(name,Boolean.toString(val));
  }

  public void writeFloat(String name, String val) throws IOException {
    writePrim("float",name,val,false);
  }

  public void writeFloat(String name, float val) throws IOException {
    writeFloat(name,Float.toString(val));
  }

  public void writeDouble(String name, String val) throws IOException {
    writePrim("double",name,val,false);
  }

  public void writeDouble(String name, double val) throws IOException {
    writeDouble(name,Double.toString(val));
  }

  public void writeDate(String name, Date val) throws IOException {
    // using a stringBuilder for numbers can be nice since
    // a temporary string isn't used (it's added directly to the
    // builder's buffer.

    cal.setTime(val);

    sb.setLength(0);
    int i = cal.get(Calendar.YEAR);
    sb.append(i);
    sb.append('-');
    i = cal.get(Calendar.MONTH) + 1;  // 0 based, so add 1
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append('-');
    i=cal.get(Calendar.DAY_OF_MONTH);
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append('T');
    i=cal.get(Calendar.HOUR_OF_DAY); // 24 hour time format
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append(':');
    i=cal.get(Calendar.MINUTE);
    if (i<10) sb.append('0');
    sb.append(i);
    sb.append(':');
    i=cal.get(Calendar.SECOND);
    if (i<10) sb.append('0');
    sb.append(i);
    i=cal.get(Calendar.MILLISECOND);
    if (i != 0) {
      sb.append('.');
      if (i<100) sb.append('0');
      if (i<10) sb.append('0');
      sb.append(i);

      // handle canonical format specifying fractional
      // seconds shall not end in '0'.  Given the slowness of
      // integer div/mod, simply checking the last character
      // is probably the fastest way to check.
      int lastIdx = sb.length()-1;
      if (sb.charAt(lastIdx)=='0') {
        lastIdx--;
        if (sb.charAt(lastIdx)=='0') {
          lastIdx--;
        }
        sb.setLength(lastIdx+1);
      }

    }
    sb.append('Z');
    writeDate(name, sb.toString());
  }

  public void writeDate(String name, String val) throws IOException {
    writePrim("date",name,val,false);
  }


  //
  // OPT - specific writeInt, writeFloat, methods might be faster since
  // there would be less write calls (write("<int name=\"" + name + ... + </int>)
  //
  public void writePrim(String tag, String name, String val, boolean escape) throws IOException {
    // OPT - we could use a temp char[] (or a StringBuilder) and if the
    // size was small enough to fit (if escape==false we can calc exact size)
    // then we could put things directly in the temp buf.
    // need to see what percent of CPU this takes up first though...
    // Could test a reusable StringBuilder...

    // is this needed here???
    // Only if a fieldtype calls writeStr or something
    // with a null val instead of calling writeNull
    /***
    if (val==null) {
      if (name==null) writer.write("<null/>");
      else writer.write("<null name=\"" + name + "/>");
    }
    ***/

    int contentLen=val.length();

    startTag(tag, name, contentLen==0);
    if (contentLen==0) return;

    if (escape) {
      XML.escapeCharData(val,writer);
    } else {
      writer.write(val,0,contentLen);
    }

    writer.write("</");
    writer.write(tag);
    writer.write('>');
  }


}
