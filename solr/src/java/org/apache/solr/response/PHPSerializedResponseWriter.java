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

import java.io.Writer;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
/**
 * A description of the PHP serialization format can be found here:
 * http://www.hurring.com/scott/code/perl/serialize/
 *
 * <p>
 * In order to support PHP Serialized strings with a proper byte count, This ResponseWriter
 * must know if the Writers passed to it will result in an output of CESU-8 (UTF-8 w/o support
 * for large code points outside of the BMP)
 * <p>
 * Currently Solr assumes that all Jetty servlet containers (detected using the "jetty.home"
 * system property) use CESU-8 instead of UTF-8 (verified to the current release of 6.1.20).
 * <p>
 * In installations where Solr auto-detects incorrectly, the Solr Administrator should set the
 * "solr.phps.cesu8" system property to either "true" or "false" accordingly.
 */
public class PHPSerializedResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PHP_UTF8="text/x-php-serialized;charset=UTF-8";

  // Is this servlet container's UTF-8 encoding actually CESU-8 (i.e. lacks support for
  // large characters outside the BMP).
  boolean CESU8 = false;
  public void init(NamedList n) {
    String cesu8Setting = System.getProperty("solr.phps.cesu8");
    if (cesu8Setting != null) {
      CESU8="true".equals(cesu8Setting);
    } else {
      // guess at the setting.
      // Jetty up until 6.1.20 at least (and probably versions after) uses CESU8
      CESU8 = System.getProperty("jetty.home") != null;
    }
  }
  
 public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    PHPSerializedWriter w = new PHPSerializedWriter(writer, req, rsp, CESU8);
    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_TEXT_UTF8;
  }
}

class PHPSerializedWriter extends JSONWriter {
  final private boolean CESU8;
  final UnicodeUtil.UTF8Result utf8;

  public PHPSerializedWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp, boolean CESU8) {
    super(writer, req, rsp);
    this.CESU8 = CESU8;
    this.utf8 = CESU8 ? null : new UnicodeUtil.UTF8Result();
    // never indent serialized PHP data
    doIndent = false;
  }

  public void writeResponse() throws IOException {
    Boolean omitHeader = req.getParams().getBool(CommonParams.OMIT_HEADER);
    if(omitHeader != null && omitHeader) rsp.getValues().remove("responseHeader");
    writeNamedList(null, rsp.getValues());
  }
  
  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {
    writeNamedListAsMapMangled(name,val);
  }
  
  @Override
  public void writeDoc(String name, Collection<Fieldable> fields, Set<String> returnFields, Map pseudoFields) throws IOException {
    ArrayList<Fieldable> single = new ArrayList<Fieldable>();
    HashMap<String, MultiValueField> multi = new HashMap<String, MultiValueField>();

    for (Fieldable ff : fields) {
      String fname = ff.name();
      if (returnFields!=null && !returnFields.contains(fname)) {
        continue;
      }
      // if the field is multivalued, it may have other values further on... so
      // build up a list for each multi-valued field.
      SchemaField sf = schema.getField(fname);
      if (sf.multiValued()) {
        MultiValueField mf = multi.get(fname);
        if (mf==null) {
          mf = new MultiValueField(sf, ff);
          multi.put(fname, mf);
        } else {
          mf.fields.add(ff);
        }
      } else {
        single.add(ff);
      }
    }

    // obtain number of fields in doc
    writeArrayOpener(single.size() + multi.size() + ((pseudoFields!=null) ? pseudoFields.size() : 0));

    // output single value fields
    for(Fieldable ff : single) {
      SchemaField sf = schema.getField(ff.name());
      writeKey(ff.name(),true);
      sf.write(this, ff.name(), ff);
    }
    
    // output multi value fields
    for(MultiValueField mvf : multi.values()) {
      writeKey(mvf.sfield.getName(), true);
      writeArrayOpener(mvf.fields.size());
      int i = 0;
      for (Fieldable ff : mvf.fields) {
        writeKey(i++, false);
        mvf.sfield.write(this, null, ff);
      }
      writeArrayCloser();
    }

    // output pseudo fields
    if (pseudoFields !=null && pseudoFields.size()>0) {
      writeMap(null,pseudoFields,true,false);
    }
    writeArrayCloser();
  }
  
  @Override
  public void writeDocList(String name, DocList ids, Set<String> fields, Map otherFields) throws IOException {
    boolean includeScore=false;
    
    if (fields!=null) {
      includeScore = fields.contains("score");
      if (fields.size()==0 || (fields.size()==1 && includeScore) || fields.contains("*")) {
        fields=null;  // null means return all stored fields
      }
    }

    int sz=ids.size();

    writeMapOpener(includeScore ? 4 : 3);
    writeKey("numFound",false);
    writeInt(null,ids.matches());
    writeKey("start",false);
    writeInt(null,ids.offset());

    if (includeScore) {
      writeKey("maxScore",false);
      writeFloat(null,ids.maxScore());
    }
    writeKey("docs",false);
    writeArrayOpener(sz);

    SolrIndexSearcher searcher = req.getSearcher();
    DocIterator iterator = ids.iterator();
    for (int i=0; i<sz; i++) {
      int id = iterator.nextDoc();
      Document doc = searcher.doc(id, fields);
      writeKey(i, false);
      writeDoc(null, doc, fields, (includeScore ? iterator.score() : 0.0f), includeScore);
    }
    writeMapCloser();

    if (otherFields !=null) {
      writeMap(null, otherFields, true, false);
    }

    writeMapCloser();
  }
  
  @Override
  public void writeSolrDocument(String name, SolrDocument doc, Set<String> returnFields, Map pseudoFields) throws IOException {
    HashMap <String,Object> single = new HashMap<String, Object>();
    HashMap <String,Object> multi = new HashMap<String, Object>();
    int pseudoSize = pseudoFields != null ? pseudoFields.size() : 0;

    for (String fname : doc.getFieldNames()) {
      if(returnFields != null && !returnFields.contains(fname)){
        continue;
      }

      Object val = doc.getFieldValue(fname);
      SchemaField sf = schema.getFieldOrNull(fname);
      if (sf != null && sf.multiValued()) {
        multi.put(fname, val);
      }else{
        single.put(fname, val);
      }
    }

    writeMapOpener(single.size() + multi.size() + pseudoSize);
    for(String fname: single.keySet()){
      Object val = single.get(fname);
      writeKey(fname, true);
      writeVal(fname, val);
    }
    
    for(String fname: multi.keySet()){
      writeKey(fname, true);

      Object val = multi.get(fname);
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

    if (pseudoSize > 0) {
      writeMap(null,pseudoFields,true, false);
    }
    writeMapCloser();
  }


  @Override
  public void writeSolrDocumentList(String name, SolrDocumentList docs, Set<String> fields, Map otherFields) throws IOException {
    boolean includeScore=false;
    if (fields!=null) {
      includeScore = fields.contains("score");
      if (fields.size()==0 || (fields.size()==1 && includeScore) || fields.contains("*")) {
        fields=null;  // null means return all stored fields
      }
    }

    int sz = docs.size();

    writeMapOpener(includeScore ? 4 : 3);

    writeKey("numFound",false);
    writeLong(null,docs.getNumFound());

    writeKey("start",false);
    writeLong(null,docs.getStart());

    if (includeScore && docs.getMaxScore() != null) {
      writeKey("maxScore",false);
      writeFloat(null,docs.getMaxScore());
    }

    writeKey("docs",false);

    writeArrayOpener(sz);
    for (int i=0; i<sz; i++) {
      writeKey(i, false);
      writeSolrDocument(null, docs.get(i), fields, otherFields);
    }
    writeArrayCloser();

    if (otherFields !=null) {
      writeMap(null, otherFields, true, false);
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
  protected void writeKey(String fname, boolean needsEscaping) throws IOException {
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
    int nBytes;
    if (CESU8) {
      nBytes = 0;
      for (int i=0; i<val.length(); i++) {
        char ch = val.charAt(i);
        if (ch<='\u007f') {
          nBytes += 1;
        } else if (ch<='\u07ff') {
          nBytes += 2;
        } else {
          nBytes += 3;
        }
      }
    } else {
      UnicodeUtil.UTF16toUTF8(val, 0, val.length(), utf8);
      nBytes = utf8.length;
    }

    writer.write("s:");
    writer.write(Integer.toString(nBytes));
    writer.write(":\"");
    writer.write(val);
    writer.write("\";");
  }
}
