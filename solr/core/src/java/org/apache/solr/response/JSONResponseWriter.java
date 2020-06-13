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
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ReturnFields;

/**
 *
 */

public class JSONResponseWriter implements QueryResponseWriter {
  public static String CONTENT_TYPE_JSON_UTF8 = "application/json; charset=UTF-8";

  private String contentType = CONTENT_TYPE_JSON_UTF8;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList namedList) {
    String contentType = (String) namedList.get("content-type");
    if (contentType != null) {
      this.contentType = contentType;
    }
  }

  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    final SolrParams params = req.getParams();
    final String wrapperFunction = params.get(JSONWriter.JSON_WRAPPER_FUNCTION);
    final String namedListStyle = params.get(JsonTextWriter.JSON_NL_STYLE, JsonTextWriter.JSON_NL_FLAT).intern();

    final JSONWriter w;
    if (namedListStyle.equals(JsonTextWriter.JSON_NL_ARROFNTV)) {
      w = new ArrayOfNameTypeValueJSONWriter(
          writer, req, rsp, wrapperFunction, namedListStyle, true);
    } else {
      w = new JSONWriter(
          writer, req, rsp, wrapperFunction, namedListStyle);
    }

    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return contentType;
  }

  public static PushWriter getPushWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    return new JSONWriter(writer, req, rsp);
  }



/**
 * Writes NamedLists directly as an array of NameTypeValue JSON objects...
 * NamedList("a"=1,"b"=null,null=3,null=null) =>
 *      [{"name":"a","type":"int","value":1},
 *       {"name":"b","type":"null","value":null},
 *       {"name":null,"type":"int","value":3},
 *       {"name":null,"type":"null","value":null}]
 * NamedList("a"=1,"bar"="foo",null=3.4f) =>
 *      [{"name":"a","type":"int","value":1},
 *      {"name":"bar","type":"str","value":"foo"},
 *      {"name":null,"type":"float","value":3.4}]
 */
class ArrayOfNameTypeValueJSONWriter extends JSONWriter {
  protected boolean writeTypeAndValueKey = false;
  private final boolean writeNullName;

  public ArrayOfNameTypeValueJSONWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp,
                                        String wrapperFunction, String namedListStyle, boolean writeNullName) {
    super(writer, req, rsp, wrapperFunction, namedListStyle);
    this.writeNullName = writeNullName;
  }

  @Override
  public void writeNamedList(String name, @SuppressWarnings({"rawtypes"})NamedList val) throws IOException {

    if (val instanceof SimpleOrderedMap) {
      super.writeNamedList(name, val);
      return;
    }

    final int sz = val.size();
    indent();

    writeArrayOpener(sz);
    incLevel();

    boolean first = true;
    for (int i=0; i<sz; i++) {
      if (first) {
        first = false;
      } else {
        writeArraySeparator();
      }

      indent();

      final String elementName = val.getName(i);
      final Object elementVal = val.getVal(i);

      /*
       * JSONWriter's writeNamedListAsArrMap turns NamedList("bar"="foo") into [{"foo":"bar"}]
       * but we here wish to turn it into [ {"name":"bar","type":"str","value":"foo"} ] instead.
       *
       * So first we write the <code>{"name":"bar",</code> portion ...
       */
      writeMapOpener(-1);
      if (elementName != null || writeNullName) {
        writeKey("name", false);
        writeVal("name", elementName);
        writeMapSeparator();
      }

      /*
       * ... and then we write the <code>"type":"str","value":"foo"}</code> portion.
       */
      writeTypeAndValueKey = true;
      writeVal(null, elementVal); // passing null since writeVal doesn't actually use name (and we already wrote elementName above)
      if (writeTypeAndValueKey) {
        throw new RuntimeException("writeTypeAndValueKey should have been reset to false by writeVal('"+elementName+"','"+elementVal+"')");
      }
      writeMapCloser();
    }

    decLevel();
    writeArrayCloser();
  }

  protected void ifNeededWriteTypeAndValueKey(String type) throws IOException {
    if (writeTypeAndValueKey) {
      writeTypeAndValueKey = false;
      writeKey("type", false);
      writeVal("type", type);
      writeMapSeparator();
      writeKey("value", false);
    }
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("int");
    super.writeInt(name, val);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("long");
    super.writeLong(name, val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("float");
    super.writeFloat(name, val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("double");
    super.writeDouble(name, val);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("bool");
    super.writeBool(name, val);
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    ifNeededWriteTypeAndValueKey("date");
    super.writeDate(name, val);
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    ifNeededWriteTypeAndValueKey("str");
    super.writeStr(name, val, needsEscaping);
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException {
    ifNeededWriteTypeAndValueKey("doc");
    super.writeSolrDocument(name, doc, returnFields, idx);
  }

  @Deprecated
  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore) throws IOException {
    ifNeededWriteTypeAndValueKey("doclist");
    super.writeStartDocumentList(name, start, size, numFound, maxScore);
  }
  
  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException {
    ifNeededWriteTypeAndValueKey("doclist");
    super.writeStartDocumentList(name, start, size, numFound, maxScore, numFoundExact);
  }


  @Override
  public void writeMap(String name, @SuppressWarnings({"rawtypes"})Map val,
                       boolean excludeOuter, boolean isFirstVal) throws IOException {
    ifNeededWriteTypeAndValueKey("map");
    super.writeMap(name, val, excludeOuter, isFirstVal);
  }

  @Override
  public void writeArray(String name, @SuppressWarnings({"rawtypes"})Iterator val) throws IOException {
    ifNeededWriteTypeAndValueKey("array");
    super.writeArray(name, val);
  }

  @Override
  public void writeNull(String name) throws IOException {
    ifNeededWriteTypeAndValueKey("null");
    super.writeNull(name);
  }
}

abstract static class NaNFloatWriter extends JSONWriter {

  abstract protected String getNaN();

  abstract protected String getInf();

  public NaNFloatWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }

  @Override
  public void writeFloat(String name, float val) throws IOException {
    if (Float.isNaN(val)) {
      writer.write(getNaN());
    } else if (Float.isInfinite(val)) {
      if (val < 0.0f)
        writer.write('-');
      writer.write(getInf());
    } else {
      writeFloat(name, Float.toString(val));
    }
  }

  @Override
  public void writeDouble(String name, double val) throws IOException {
    if (Double.isNaN(val)) {
      writer.write(getNaN());
    } else if (Double.isInfinite(val)) {
      if (val < 0.0)
        writer.write('-');
      writer.write(getInf());
    } else {
      writeDouble(name, Double.toString(val));
    }
  }
}
}
