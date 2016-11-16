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

import java.io.Writer;
import java.io.IOException;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

public class PHPResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PHP_UTF8="text/x-php;charset=UTF-8";

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
    PHPWriter w = new PHPWriter(writer, req, rsp);
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

class PHPWriter extends JSONWriter {
  public PHPWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }
  
  @Override
  public void writeNamedList(String name, NamedList val) throws IOException {
    writeNamedListAsMapMangled(name,val);
  }

  @Override
  public void writeMapOpener(int size) throws IOException {
    writer.write("array(");
  }

  @Override
  public void writeMapCloser() throws IOException {
    writer.write(')');
  }

  @Override
  public void writeArrayOpener(int size) throws IOException {
    writer.write("array(");
  }

  @Override
  public void writeArray(String name, List l) throws IOException {
    writeArray(name,l.iterator());
  }

  @Override
  public void writeArrayCloser() throws IOException {
    writer.write(')');
  }

  @Override
  public void writeNull(String name) throws IOException {
    writer.write("null");
  }

  @Override
  protected void writeKey(String fname, boolean needsEscaping) throws IOException {
    writeStr(null, fname, needsEscaping);
    writer.write('=');
    writer.write('>');
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    if (needsEscaping) {
      writer.write('\'');
      for (int i=0; i<val.length(); i++) {
        char ch = val.charAt(i);
        switch (ch) {
          case '\'':
          case '\\': writer.write('\\'); writer.write(ch); break;
          default:
            writer.write(ch);
        }
      }
      writer.write('\'');
    } else {
      writer.write('\'');
      writer.write(val);
      writer.write('\'');
    }
  }
}
