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
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ReturnFields;

public class JSONWriter extends TextResponseWriter implements JsonTextWriter {
  static final int    JSON_NL_STYLE_COUNT = 5; // for use by JSONWriterTest
  static final String JSON_WRAPPER_FUNCTION="json.wrf";

  final protected String namedListStyle;
  protected String wrapperFunction;

  public JSONWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    this(writer, req, rsp,
        req.getParams().get(JSON_WRAPPER_FUNCTION),
        req.getParams().get(JSON_NL_STYLE, JSON_NL_FLAT).intern());
  }

  public JSONWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp,
                    String wrapperFunction, String namedListStyle) {
    super(writer, req, rsp);
    this.wrapperFunction = wrapperFunction;
    this.namedListStyle = namedListStyle;
  }
  private JSONWriter(Writer writer, boolean indent, String namedListStyle) throws IOException {
    super(writer, indent);
    this.namedListStyle = namedListStyle;

  }

  /**Strictly for testing only
   */
  public static void write(Writer writer, boolean indent,  String namedListStyle, Object val) throws IOException {
    JSONWriter jw = new JSONWriter(writer, indent, namedListStyle);
    jw.writeVal(null, val);
    jw.close();

  }

  @Override
  public String getNamedListStyle() {
    return namedListStyle;
  }


  public void writeResponse() throws IOException {
    if(wrapperFunction!=null) {
      _writeStr(wrapperFunction + "(");
    }
    writeNamedList(null, rsp.getValues());
    if(wrapperFunction!=null) {
      _writeChar(')');
    }
    _writeChar('\n');  // ending with a newline looks much better from the command line
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException {
    if( idx > 0 ) {
      writeArraySeparator();
    }

    indent();
    writeMapOpener(doc.size());
    incLevel();

    boolean first=true;
    for (String fname : doc.getFieldNames()) {
      if (returnFields!= null && !returnFields.wantsField(fname)) {
        continue;
      }

      if (first) {
        first=false;
      }
      else {
        writeMapSeparator();
      }

      indent();
      writeKey(fname, true);
      Object val = doc.getFieldValue(fname);
      writeVal(fname, val);
    }

    if(doc.hasChildDocuments()) {
      if(first == false) {
        writeMapSeparator();
        indent();
      }
      writeKey("_childDocuments_", true);
      writeArrayOpener(doc.getChildDocumentCount());
      List<SolrDocument> childDocs = doc.getChildDocuments();
      for(int i=0; i<childDocs.size(); i++) {
        writeSolrDocument(null, childDocs.get(i), null, i);
      }
      writeArrayCloser();
    }

    decLevel();
    writeMapCloser();
  }


  //
  // Data structure tokens
  // NOTE: a positive size paramater indicates the number of elements
  //       contained in an array or map, a negative value indicates
  //       that the size could not be reliably determined.
  //

  /**
   * This method will be removed in Solr 9
   * @deprecated Use {{@link #writeStartDocumentList(String, long, int, long, Float, Boolean)}.
   */
  @Override
  @Deprecated
  public void writeStartDocumentList(String name,
      long start, int size, long numFound, Float maxScore) throws IOException
  {
    writeMapOpener(headerSize(maxScore, null));
    incLevel();
    writeKey("numFound",false);
    writeLong(null,numFound);
    writeMapSeparator();
    writeKey("start",false);
    writeLong(null,start);

    if (maxScore!=null) {
      writeMapSeparator();
      writeKey("maxScore",false);
      writeFloat(null,maxScore);
    }
    writeMapSeparator();
    // indent();
    writeKey("docs",false);
    writeArrayOpener(size);

    incLevel();
  }
  
  @Override
  public void writeStartDocumentList(String name,
      long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException {
    writeMapOpener(headerSize(maxScore, numFoundExact));
    incLevel();
    writeKey("numFound",false);
    writeLong(null,numFound);
    writeMapSeparator();
    writeKey("start",false);
    writeLong(null,start);

    if (maxScore != null) {
      writeMapSeparator();
      writeKey("maxScore",false);
      writeFloat(null,maxScore);
    }
    
    if (numFoundExact != null) {
      writeMapSeparator();
      writeKey("numFoundExact",false);
      writeBool(null, numFoundExact);
    }
    writeMapSeparator();
    writeKey("docs",false);
    writeArrayOpener(size);

    incLevel();
  } 

  protected int headerSize(Float maxScore, Boolean numFoundExact) {
    int headerSize = 3;
    if (maxScore != null) headerSize++;
    if (numFoundExact != null) headerSize++;
    return headerSize;
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    decLevel();
    writeArrayCloser();

    decLevel();
    indent();
    writeMapCloser();
  }

  @Override
  public void _writeChar(char c) throws IOException {
    writer.write(c);
  }

  @Override
  public void _writeStr(String s) throws IOException {
    writer.write(s);
  }

}
