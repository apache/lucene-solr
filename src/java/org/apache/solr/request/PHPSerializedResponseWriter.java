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

import java.io.Writer;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * A description of the PHP serialization format can be found here:
 * http://www.hurring.com/scott/code/perl/serialize/
 */

public class PHPSerializedResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PHP_UTF8="text/x-php-serialized;charset=UTF-8";

  public void init(NamedList n) {
    /* NOOP */
  }
  
 public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    PHPSerializedWriter w = new PHPSerializedWriter(writer, req, rsp);
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
  public PHPSerializedWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
    // never indent serialized PHP data
    doIndent = false;
  }

  public void writeResponse() throws IOException {
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
    writer.write("s:"+val.getBytes("UTF8").length+":\""+val+"\";");
  }
}
