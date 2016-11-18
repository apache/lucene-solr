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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.SolrParams;
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
  public void init(NamedList namedList) {
    String contentType = (String) namedList.get("content-type");
    if (contentType != null) {
      this.contentType = contentType;
    }
  }

  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    final SolrParams params = req.getParams();
    final String wrapperFunction = params.get(JSONWriter.JSON_WRAPPER_FUNCTION);
    final String namedListStyle = params.get(JSONWriter.JSON_NL_STYLE, JSONWriter.JSON_NL_FLAT).intern();

    final JSONWriter w;
    if (namedListStyle.equals(JSONWriter.JSON_NL_ARROFNVP)) {
      w = new ArrayOfNamedValuePairJSONWriter(
          writer, req, rsp, wrapperFunction, namedListStyle);
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

}

class JSONWriter extends TextResponseWriter {
  protected String wrapperFunction;
  final protected String namedListStyle;

  static final String JSON_NL_STYLE="json.nl";
  static final int    JSON_NL_STYLE_COUNT = 5; // for use by JSONWriterTest

  static final String JSON_NL_MAP="map";
  static final String JSON_NL_FLAT="flat";
  static final String JSON_NL_ARROFARR="arrarr";
  static final String JSON_NL_ARROFMAP="arrmap";
  static final String JSON_NL_ARROFNVP="arrnvp";

  static final String JSON_WRAPPER_FUNCTION="json.wrf";

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

  public void writeResponse() throws IOException {
    if(wrapperFunction!=null) {
        writer.write(wrapperFunction + "(");
    }
    writeNamedList(null, rsp.getValues());
    if(wrapperFunction!=null) {
        writer.write(')');
    }
    writer.write('\n');  // ending with a newline looks much better from the command line
  }

  protected void writeKey(String fname, boolean needsEscaping) throws IOException {
    writeStr(null, fname, needsEscaping);
    writer.write(':');
  }

  /** Represents a NamedList directly as a JSON Object (essentially a Map)
   * Map null to "" and name mangle any repeated keys to avoid repeats in the
   * output.
   */
  protected void writeNamedListAsMapMangled(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeMapOpener(sz);
    incLevel();

    // In JSON objects (maps) we can't have null keys or duplicates...
    // map null to "" and append a qualifier to duplicates.
    //
    // a=123,a=456 will be mapped to {a=1,a__1=456}
    // Disad: this is ambiguous since a real key could be called a__1
    //
    // Another possible mapping could aggregate multiple keys to an array:
    // a=123,a=456 maps to a=[123,456]
    // Disad: this is ambiguous with a real single value that happens to be an array
    //
    // Both of these mappings have ambiguities.
    HashMap<String,Integer> repeats = new HashMap<>(4);

    boolean first=true;
    for (int i=0; i<sz; i++) {
      String key = val.getName(i);
      if (key==null) key="";

      if (first) {
        first=false;
        repeats.put(key,0);
      } else {
        writeMapSeparator();

        Integer repeatCount = repeats.get(key);
        if (repeatCount==null) {
          repeats.put(key,0);
        } else {
          String newKey = key;
          int newCount = repeatCount;
          do {  // avoid generated key clashing with a real key
            newKey = key + ' ' + (++newCount);
            repeatCount = repeats.get(newKey);
          } while (repeatCount != null);

          repeats.put(key,newCount);
          key = newKey;
        }
      }

      indent();
      writeKey(key, true);
      writeVal(key,val.getVal(i));
    }

    decLevel();
    writeMapCloser();
  }

  /** Represents a NamedList directly as a JSON Object (essentially a Map)
   * repeating any keys if they are repeated in the NamedList.
   * null key is mapped to "".
   */ 
  // NamedList("a"=1,"bar"="foo",null=3,null=null) => {"a":1,"bar":"foo","":3,"":null}
  protected void writeNamedListAsMapWithDups(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeMapOpener(sz);
    incLevel();

    for (int i=0; i<sz; i++) {
      if (i!=0) {
        writeMapSeparator();
      }

      String key = val.getName(i);
      if (key==null) key="";
      indent();
      writeKey(key, true);
      writeVal(key,val.getVal(i));
    }

    decLevel();
    writeMapCloser();
  }

  // Represents a NamedList directly as an array of JSON objects...
  // NamedList("a"=1,"b"=2,null=3,null=null) => [{"a":1},{"b":2},3,null]
  protected void writeNamedListAsArrMap(String name, NamedList val) throws IOException {
    int sz = val.size();
    indent();
    writeArrayOpener(sz);
    incLevel();

    boolean first=true;
    for (int i=0; i<sz; i++) {
      String key = val.getName(i);

      if (first) {
        first=false;
      } else {
        writeArraySeparator();
      }

      indent();

      if (key==null) {
        writeVal(null,val.getVal(i));
      } else {
        writeMapOpener(1);
        writeKey(key, true);
        writeVal(key,val.getVal(i));
        writeMapCloser();
      }

    }

    decLevel();
    writeArrayCloser();
  }

  // Represents a NamedList directly as an array of JSON objects...
  // NamedList("a"=1,"b"=2,null=3,null=null) => [["a",1],["b",2],[null,3],[null,null]]
  protected void writeNamedListAsArrArr(String name, NamedList val) throws IOException {
    int sz = val.size();
    indent();
    writeArrayOpener(sz);
    incLevel();

    boolean first=true;
    for (int i=0; i<sz; i++) {
      String key = val.getName(i);

      if (first) {
        first=false;
      } else {
        writeArraySeparator();
      }

      indent();

      /*** if key is null, just write value???
      if (key==null) {
        writeVal(null,val.getVal(i));
      } else {
     ***/

        writeArrayOpener(1);
        incLevel();
        if (key==null) {
          writeNull(null);
        } else {
          writeStr(null, key, true);
        }
        writeArraySeparator();
        writeVal(key,val.getVal(i));
        decLevel();
        writeArrayCloser();
    }

    decLevel();
    writeArrayCloser();
  }

  // Represents a NamedList directly as an array with keys/values
  // interleaved.
  // NamedList("a"=1,"b"=2,null=3,null=null) => ["a",1,"b",2,null,3,null,null]
  protected void writeNamedListAsFlat(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeArrayOpener(sz*2);
    incLevel();

    for (int i=0; i<sz; i++) {
      if (i!=0) {
        writeArraySeparator();
      }
      String key = val.getName(i);
      indent();
      if (key==null) {
        writeNull(null);
      } else {
        writeStr(null, key, true);
      }
      writeArraySeparator();
      writeVal(key, val.getVal(i));
    }

    decLevel();
    writeArrayCloser();
  }


  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {
    if (val instanceof SimpleOrderedMap) {
      writeNamedListAsMapWithDups(name,val);
    } else if (namedListStyle==JSON_NL_FLAT) {
      writeNamedListAsFlat(name,val);
    } else if (namedListStyle==JSON_NL_MAP){
      writeNamedListAsMapWithDups(name,val);
    } else if (namedListStyle==JSON_NL_ARROFARR) {
      writeNamedListAsArrArr(name,val);
    } else if (namedListStyle==JSON_NL_ARROFMAP) {
      writeNamedListAsArrMap(name,val);
    } else if (namedListStyle==JSON_NL_ARROFNVP) {
      throw new UnsupportedOperationException(namedListStyle
          + " namedListStyle must only be used with "+ArrayOfNamedValuePairJSONWriter.class.getSimpleName());
    }
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

      // SolrDocument will now have multiValued fields represented as a Collection,
      // even if only a single value is returned for this document.
      if (val instanceof List) {
        // shortcut this common case instead of going through writeVal again
        writeArray(name,((Iterable)val).iterator());
      } else {
        writeVal(fname, val);
      }
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

  @Override
  public void writeStartDocumentList(String name, 
      long start, int size, long numFound, Float maxScore) throws IOException
  {
    writeMapOpener((maxScore==null) ? 3 : 4);
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
  public void writeEndDocumentList() throws IOException
  {
    decLevel();
    writeArrayCloser();

    decLevel();
    indent();
    writeMapCloser();
  }


  //
  // Data structure tokens
  // NOTE: a positive size paramater indicates the number of elements
  //       contained in an array or map, a negative value indicates 
  //       that the size could not be reliably determined.
  // 
  
  public void writeMapOpener(int size) throws IOException, IllegalArgumentException {
    writer.write('{');
  }
  
  public void writeMapSeparator() throws IOException {
    writer.write(',');
  }

  public void writeMapCloser() throws IOException {
    writer.write('}');
  }
  
  public void writeArrayOpener(int size) throws IOException, IllegalArgumentException {
    writer.write('[');
  }
  
  public void writeArraySeparator() throws IOException {
    writer.write(',');
  }

  public void writeArrayCloser() throws IOException {
    writer.write(']');
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    // it might be more efficient to use a stringbuilder or write substrings
    // if writing chars to the stream is slow.
    if (needsEscaping) {


     /* http://www.ietf.org/internet-drafts/draft-crockford-jsonorg-json-04.txt
      All Unicode characters may be placed within
      the quotation marks except for the characters which must be
      escaped: quotation mark, reverse solidus, and the control
      characters (U+0000 through U+001F).
     */
      writer.write('"');

      for (int i=0; i<val.length(); i++) {
        char ch = val.charAt(i);
        if ((ch > '#' && ch != '\\' && ch < '\u2028') || ch == ' ') { // fast path
          writer.write(ch);
          continue;
        }
        switch(ch) {
          case '"':
          case '\\':
            writer.write('\\');
            writer.write(ch);
            break;
          case '\r': writer.write('\\'); writer.write('r'); break;
          case '\n': writer.write('\\'); writer.write('n'); break;
          case '\t': writer.write('\\'); writer.write('t'); break;
          case '\b': writer.write('\\'); writer.write('b'); break;
          case '\f': writer.write('\\'); writer.write('f'); break;
          case '\u2028': // fallthrough
          case '\u2029':
            unicodeEscape(writer,ch);
            break;
          // case '/':
          default: {
            if (ch <= 0x1F) {
              unicodeEscape(writer,ch);
            } else {
              writer.write(ch);
            }
          }
        }
      }

      writer.write('"');
    } else {
      writer.write('"');
      writer.write(val);
      writer.write('"');
    }
  }

  @Override
  public void writeIterator(IteratorWriter val) throws IOException {
    writeArrayOpener(-1);
    incLevel();
    val.writeIter(new IteratorWriter.ItemWriter() {
      boolean first = true;

      @Override
      public IteratorWriter.ItemWriter add(Object o) throws IOException {
        if (!first) {
          JSONWriter.this.indent();
          JSONWriter.this.writeArraySeparator();
        }
        JSONWriter.this.writeVal(null, o);
        first = false;
        return this;
      }
    });
    decLevel();
    writeArrayCloser();
  }

  @Override
  public void writeMap(MapWriter val)
      throws IOException {
    writeMapOpener(-1);
    incLevel();

    val.writeMap(new EntryWriter() {
      boolean isFirst = true;

      @Override
      public EntryWriter put(String k, Object v) throws IOException {
        if (isFirst) {
          isFirst = false;
        } else {
          JSONWriter.this.writeMapSeparator();
        }
        if (doIndent) JSONWriter.this.indent();
        JSONWriter.this.writeKey(k, true);
        JSONWriter.this.writeVal(k, v);
        return this;
      }
    });
    decLevel();
    writeMapCloser();
  }

  @Override
  public void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException {
    if (!excludeOuter) {
      writeMapOpener(val.size());
      incLevel();
      isFirstVal=true;
    }

    boolean doIndent = excludeOuter || val.size() > 1;

    for (Map.Entry entry : (Set<Map.Entry>)val.entrySet()) {
      Object e = entry.getKey();
      String k = e==null ? "" : e.toString();
      Object v = entry.getValue();

      if (isFirstVal) {
        isFirstVal=false;
      } else {
        writeMapSeparator();
      }

      if (doIndent) indent();
      writeKey(k,true);
      writeVal(k,v);
    }

    if (!excludeOuter) {
      decLevel();
      writeMapCloser();
    }
  }

  @Override
  public void writeArray(String name, List l) throws IOException {
    writeArrayOpener(l.size());
    writeJsonIter(l.iterator());
    writeArrayCloser();
  }

  @Override
  public void writeArray(String name, Iterator val) throws IOException {
    writeArrayOpener(-1); // no trivial way to determine array size
    writeJsonIter(val);
    writeArrayCloser();
  }

  private void writeJsonIter(Iterator val) throws IOException {
    incLevel();
    boolean first=true;
    while( val.hasNext() ) {
      if( !first ) indent();
      writeVal(null, val.next());
      if( val.hasNext() ) {
        writeArraySeparator();
      }
      first=false;
    }
    decLevel();
  }

  //
  // Primitive types
  //
  @Override
  public void writeNull(String name) throws IOException {
    writer.write("null");
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    writer.write(val);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    writer.write(val);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    writer.write(val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    writer.write(val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    writer.write(val);
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    writeStr(name, val, false);
  }

  private static char[] hexdigits = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
  protected static void unicodeEscape(Appendable out, int ch) throws IOException {
    out.append('\\');
    out.append('u');
    out.append(hexdigits[(ch>>>12)     ]);
    out.append(hexdigits[(ch>>>8) & 0xf]);
    out.append(hexdigits[(ch>>>4) & 0xf]);
    out.append(hexdigits[(ch)     & 0xf]);
  }

}

/**
 * Writes NamedLists directly as an array of NamedValuePair JSON objects...
 * NamedList("a"=1,"b"=2,null=3,null=null) => [{"name":"a","int":1},{"name":"b","int":2},{"int":3},{"null":null}]
 * NamedList("a"=1,"bar"="foo",null=3.4f) => [{"name":"a","int":1},{"name":"bar","str":"foo"},{"float":3.4}]
 */
class ArrayOfNamedValuePairJSONWriter extends JSONWriter {
  private boolean writeTypeAsKey = false;

  public ArrayOfNamedValuePairJSONWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp,
                                         String wrapperFunction, String namedListStyle) {
    super(writer, req, rsp, wrapperFunction, namedListStyle);
    if (namedListStyle != JSON_NL_ARROFNVP) {
      throw new UnsupportedOperationException(ArrayOfNamedValuePairJSONWriter.class.getSimpleName()+" must only be used with "
          + JSON_NL_ARROFNVP + " style");
    }
  }

  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {

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
       * but we here wish to turn it into [ {"name":"bar","str":"foo"} ] instead.
       *
       * So first we write the <code>{"name":"bar",</code> portion ...
       */
      writeMapOpener(-1);
      if (elementName != null) {
        writeKey("name", false);
        writeVal("name", elementName);
        writeMapSeparator();
      }

      /*
       * ... and then we write the <code>"str":"foo"}</code> portion.
       */
      writeTypeAsKey = true;
      writeVal(null, elementVal); // passing null since writeVal doesn't actually use name (and we already wrote elementName above)
      if (writeTypeAsKey) {
        throw new RuntimeException("writeTypeAsKey should have been reset to false by writeVal('"+elementName+"','"+elementVal+"')");
      }
      writeMapCloser();
    }

    decLevel();
    writeArrayCloser();
  }

  private void ifNeededWriteTypeAsKey(String type) throws IOException {
    if (writeTypeAsKey) {
      writeTypeAsKey = false;
      writeKey(type, false);
    }
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("int");
    super.writeInt(name, val);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("long");
    super.writeLong(name, val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("float");
    super.writeFloat(name, val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("double");
    super.writeDouble(name, val);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("bool");
    super.writeBool(name, val);
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    ifNeededWriteTypeAsKey("date");
    super.writeDate(name, val);
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    ifNeededWriteTypeAsKey("str");
    super.writeStr(name, val, needsEscaping);
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException {
    ifNeededWriteTypeAsKey("doc");
    super.writeSolrDocument(name, doc, returnFields, idx);
  }

  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore) throws IOException {
    ifNeededWriteTypeAsKey("doclist");
    super.writeStartDocumentList(name, start, size, numFound, maxScore);
  }

  @Override
  public void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException {
    ifNeededWriteTypeAsKey("map");
    super.writeMap(name, val, excludeOuter, isFirstVal);
  }

  @Override
  public void writeArray(String name, Iterator val) throws IOException {
    ifNeededWriteTypeAsKey("array");
    super.writeArray(name, val);
  }

  @Override
  public void writeNull(String name) throws IOException {
    ifNeededWriteTypeAsKey("null");
    super.writeNull(name);
  }
}

abstract class NaNFloatWriter extends JSONWriter {
  
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
