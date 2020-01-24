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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ReturnFields;


/**
 * A description of the PHP serialization format can be found here:
 * http://www.hurring.com/scott/code/perl/serialize/
 */
public class PHPSerializedResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PHP_UTF8="text/x-php-serialized;charset=UTF-8";

  private String contentType = CONTENT_TYPE_PHP_UTF8;

  @Override
  public void init(NamedList namedList) {
    String contentType = (String) namedList.get("content-type");
    if (contentType != null) {
      this.contentType = contentType;
    }
  }
  
  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    PHPSerializedWriter w = new PHPSerializedWriter(writer, req, rsp);
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
}

class PHPSerializedWriter extends JSONWriter {
  byte[] utf8;

  public PHPSerializedWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
    this.utf8 = BytesRef.EMPTY_BYTES;
    // never indent serialized PHP data
    doIndent = false;
  }

  @Override
  public void writeResponse() throws IOException {
    Boolean omitHeader = req.getParams().getBool(CommonParams.OMIT_HEADER);
    if(omitHeader != null && omitHeader) rsp.removeResponseHeader();
    writeNamedList(null, rsp.getValues());
  }
  
  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {
    writeNamedListAsMapMangled(name,val);
  }
  
  

  @Override
  public void writeStartDocumentList(String name, 
      long start, int size, long numFound, Float maxScore) throws IOException
  {
    writeMapOpener((maxScore==null) ? 3 : 4);
    writeKey("numFound",false);
    writeLong(null,numFound);
    writeKey("start",false);
    writeLong(null,start);

    if (maxScore!=null) {
      writeKey("maxScore",false);
      writeFloat(null,maxScore);
    }
    writeKey("docs",false);
    writeArrayOpener(size);
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    writeArrayCloser(); // doc list
    writeMapCloser();
  }
  
  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException
  {
    writeKey(idx, false);
    
    LinkedHashMap <String,Object> single = new LinkedHashMap<>();
    LinkedHashMap <String,Object> multi = new LinkedHashMap<>();

    for (String fname : doc.getFieldNames()) {
      if (returnFields != null && !returnFields.wantsField(fname)) {
        continue;
      }

      Object val = doc.getFieldValue(fname);
      if (val instanceof Collection) {
        multi.put(fname, val);
      }else{
        single.put(fname, val);
      }
    }

    writeMapOpener(single.size() + multi.size());
    for(Map.Entry<String, Object> entry : single.entrySet()){
      String fname = entry.getKey();
      Object val = entry.getValue();
      writeKey(fname, true);
      writeVal(fname, val);
    }
    
    for(Map.Entry<String, Object> entry : multi.entrySet()){
      String fname = entry.getKey();
      writeKey(fname, true);

      Object val = entry.getValue();
      if (!(val instanceof Collection)) {
        // should never be reached if multivalued fields are stored as a Collection
        // so I'm assuming a size of 1 just to wrap the single value
        writeArrayOpener(1);
        writeVal(fname, val);
        writeArrayCloser();
      }else{
        writeVal(fname, val);
      }
    }
    
    writeMapCloser();
  }

  
  @Override
  public void writeArray(String name, Object[] val) throws IOException {
    writeMapOpener(val.length);
    for(int i=0; i < val.length; i++) {
      writeKey(i, false);
      writeVal(String.valueOf(i), val[i]);
    }
    writeMapCloser();
  }

  @Override
  public void writeArray(String name, Iterator val) throws IOException {
    ArrayList vals = new ArrayList();
    while( val.hasNext() ) {
      vals.add(val.next());
    }
    writeArray(name, vals.toArray());
  }
  
  @Override
  public void writeMapOpener(int size) throws IOException, IllegalArgumentException {
    // negative size value indicates that something has gone wrong
    if (size < 0) {
      throw new IllegalArgumentException("Map size must not be negative");
    }
    writer.write("a:"+size+":{");
  }
  
  @Override
  public void writeMapSeparator() throws IOException {
    /* NOOP */
  }

  @Override
  public void writeMapCloser() throws IOException {
    writer.write('}');
  }

  @Override
  public void writeArrayOpener(int size) throws IOException, IllegalArgumentException {
    // negative size value indicates that something has gone wrong
    if (size < 0) {
      throw new IllegalArgumentException("Array size must not be negative");
    }
    writer.write("a:"+size+":{");
  }

  @Override  
  public void writeArraySeparator() throws IOException {
    /* NOOP */
  }

  @Override
  public void writeArrayCloser() throws IOException {
    writer.write('}');
  }
  
  @Override
  public void writeNull(String name) throws IOException {
    writer.write("N;");
  }

  @Override
  public void writeKey(String fname, boolean needsEscaping) throws IOException {
    writeStr(null, fname, needsEscaping);
  }
  void writeKey(int val, boolean needsEscaping) throws IOException {
    writeInt(null, String.valueOf(val));
  }

  @Override
  public void writeBool(String name, boolean val) throws IOException {
    writer.write(val ? "b:1;" : "b:0;");
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    writeBool(name, val.charAt(0) == 't');
  }
  
  @Override
  public void writeInt(String name, String val) throws IOException {
    writer.write("i:"+val+";");
  }
  
  @Override
  public void writeLong(String name, String val) throws IOException {
    writeInt(name,val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    writeDouble(name,val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    writer.write("d:"+val+";");
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    // serialized PHP strings don't need to be escaped at all, however the 
    // string size reported needs be the number of bytes rather than chars.
    utf8 = ArrayUtil.grow(utf8, val.length() * UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR);
    final int nBytes = UnicodeUtil.UTF16toUTF8(val, 0, val.length(), utf8);

    writer.write("s:");
    writer.write(Integer.toString(nBytes));
    writer.write(":\"");
    writer.write(val);
    writer.write("\";");
  }
}
