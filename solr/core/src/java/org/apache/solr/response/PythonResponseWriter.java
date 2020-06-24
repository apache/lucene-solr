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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

public class PythonResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PYTHON_ASCII="text/x-python;charset=US-ASCII";

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList n) {
    /* NOOP */
  }
  
  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    PythonWriter w = new PythonWriter(writer, req, rsp);
    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_TEXT_ASCII;
  }
}

class PythonWriter extends JSONResponseWriter.NaNFloatWriter {
  @Override
  protected String getNaN() { return "float('NaN')"; }
  @Override
  protected String getInf() { return "float('Inf')"; }

  public PythonWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }

  @Override
  public void writeNull(String name) throws IOException {
    writer.write("None");
  }

  @Override
  public void writeBool(String name, boolean val) throws IOException {
    writer.write(val ? "True" : "False");
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    writeBool(name,val.charAt(0)=='t');
  }

  /* optionally use a unicode python string if necessary */
  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    if (!needsEscaping) {
      writer.write('\'');
      writer.write(val);
      writer.write('\'');
      return;
    }

    // use python unicode strings...
    // python doesn't tolerate newlines in strings in its eval(), so we must escape them.

    StringBuilder sb = new StringBuilder(val.length());
    boolean needUnicode=false;

    for (int i=0; i<val.length(); i++) {
      char ch = val.charAt(i);
      switch(ch) {
        case '\'':
        case '\\': sb.append('\\'); sb.append(ch); break;
        case '\r': sb.append("\\r"); break;
        case '\n': sb.append("\\n"); break;
        case '\t': sb.append("\\t"); break;
        default:
          // we don't strictly have to escape these chars, but it will probably increase
          // portability to stick to visible ascii
          if (ch<' ' || ch>127) {
            unicodeEscape(sb, ch);
            needUnicode=true;
          } else {
            sb.append(ch);
          }
      }
    }

    if (needUnicode) {
      writer.write('u');
    }
    writer.write('\'');
    writer.append(sb);
    writer.write('\'');
  }

  /*
  old version that always used unicode
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    // use python unicode strings...
    // python doesn't tolerate newlines in strings in its eval(), so we must escape them.
    writer.write("u'");
    // it might be more efficient to use a stringbuilder or write substrings
    // if writing chars to the stream is slow.
    if (needsEscaping) {
      for (int i=0; i<val.length(); i++) {
        char ch = val.charAt(i);
        switch(ch) {
          case '\'':
          case '\\': writer.write('\\'); writer.write(ch); break;
          case '\r': writer.write("\\r"); break;
          case '\n': writer.write("\\n"); break;
          default:
            // we don't strictly have to escape these chars, but it will probably increase
            // portability to stick to visible ascii
            if (ch<' ' || ch>127) {
              unicodeChar(ch);
            } else {
              writer.write(ch);
            }
        }
      }
    } else {
      writer.write(val);
    }
    writer.write('\'');
  }
  */

}
