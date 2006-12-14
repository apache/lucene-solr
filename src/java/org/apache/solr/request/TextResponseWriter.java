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

import org.apache.lucene.document.Document;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.NamedList;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/** Base class for text-oriented response writers.
 *
 * @author yonik
 * @version $Id$
 */
public abstract class TextResponseWriter {
  
  protected final Writer writer;
  protected final IndexSchema schema;
  protected final SolrIndexSearcher searcher;
  protected final SolrQueryRequest req;
  protected final SolrQueryResponse rsp;

  // the default set of fields to return for each document
  protected Set<String> returnFields;

  protected int level;
  protected boolean doIndent;


  public TextResponseWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.writer = writer;
    this.schema = req.getSchema();
    this.searcher = req.getSearcher();
    this.req = req;
    this.rsp = rsp;
    String indent = req.getParam("indent");
    if (indent != null && !"".equals(indent) && !"off".equals(indent)) {
      doIndent=true;
    }
    returnFields = rsp.getReturnFields();
  }

  /** returns the Writer that the response is being written to */
  public Writer getWriter() { return writer; }

  // use a combination of tabs and spaces to minimize the size of an indented response.
  private static final String[] indentArr = new String[] {
    "\n",
    "\n ",
    "\n  ",
    "\n\t",
    "\n\t ",
    "\n\t  ",  // could skip this one (the only 3 char seq)
    "\n\t\t",
    "\n\t\t "};

  public void indent() throws IOException {
     if (doIndent) indent(level);
  }

  public void indent(int lev) throws IOException {
    int arrsz = indentArr.length-1;
    // power-of-two intent array (gratuitous optimization :-)
    String istr = indentArr[lev & (indentArr.length-1)];
    writer.write(istr);
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
  }


  public abstract void writeNamedList(String name, NamedList val) throws IOException;

  public void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)

    // go in order of most common to least common
    if (val==null) {
      writeNull(name);
    } else if (val instanceof String) {
      writeStr(name, val.toString(), true);
      // micro-optimization... using toString() avoids a cast first
    } else if (val instanceof Integer) {
      writeInt(name, val.toString());
    } else if (val instanceof Boolean) {
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
      writeDocList(name, (DocList)val, returnFields,null);
    // }
    // else if (val instanceof DocSet) {
    // how do we know what fields to read?
    // todo: have a DocList/DocSet wrapper that
    // restricts the fields to write...?
    } else if (val instanceof Map) {
      writeMap(name, (Map)val, false, true);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Collection) {
      writeArray(name,(Collection)val);
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else {
      // default... for debugging only
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }

  // names are passed when writing primitives like writeInt to allow many different
  // types of formats, including those where the name may come after the value (like
  // some XML formats).

  public abstract void writeDoc(String name, Document doc, Set<String> returnFields, float score, boolean includeScore) throws IOException;

  public abstract void writeDocList(String name, DocList ids, Set<String> fields, Map otherFields) throws IOException;

  public abstract void writeStr(String name, String val, boolean needsEscaping) throws IOException;

  public abstract void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException;

  public abstract void writeArray(String name, Object[] val) throws IOException;

  public abstract void writeArray(String name, Collection val) throws IOException;

  public abstract void writeNull(String name) throws IOException;

  /** if this form of the method is called, val is the Java string form of an int */
  public abstract void writeInt(String name, String val) throws IOException;

  public void writeInt(String name, int val) throws IOException {
    writeInt(name,Integer.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a long */
  public abstract void writeLong(String name, String val) throws IOException;

  public  void writeLong(String name, long val) throws IOException {
    writeLong(name,Long.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a boolean */
  public abstract void writeBool(String name, String val) throws IOException;

  public void writeBool(String name, boolean val) throws IOException {
    writeBool(name,Boolean.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a float */
  public abstract void writeFloat(String name, String val) throws IOException;

  public void writeFloat(String name, float val) throws IOException {
    writeFloat(name,Float.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a double */
  public abstract void writeDouble(String name, String val) throws IOException;

  public void writeDouble(String name, double val) throws IOException {
    writeDouble(name,Double.toString(val));
  }

  public abstract void writeDate(String name, Date val) throws IOException;

  /** if this form of the method is called, val is the Solr ISO8601 based date format */
  public abstract void writeDate(String name, String val) throws IOException;
}
