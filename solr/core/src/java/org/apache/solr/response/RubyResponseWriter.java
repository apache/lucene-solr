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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

public class RubyResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_RUBY_UTF8="text/x-ruby;charset=UTF-8";

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList n) {
    /* NOOP */
  }
  
 @Override
public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    RubyWriter w = new RubyWriter(writer, req, rsp);
    try {
      w.writeResponse();
    } finally {
      w.close();
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_TEXT_UTF8;
  }
}

class RubyWriter extends JSONResponseWriter.NaNFloatWriter {

  @Override
  protected String getNaN() { return "(0.0/0.0)"; }
  @Override
  protected String getInf() { return "(1.0/0.0)"; }

  public RubyWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }

  @Override
  public void writeNull(String name) throws IOException {
    writer.write("nil");
  }

  @Override
  public void writeKey(String fname, boolean needsEscaping) throws IOException {
    writeStr(null, fname, needsEscaping);
    writer.write('=');
    writer.write('>');
  }

  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    // Ruby doesn't do unicode escapes... so let the servlet container write raw UTF-8
    // bytes into the string.
    //
    // Use single quoted strings for safety since no evaluation is done within them.
    // Also, there are very few escapes recognized in a single quoted string, so
    // only escape the backslash and single quote.
    writer.write('\'');
    if (needsEscaping) {
      for (int i=0; i<val.length(); i++) {
        char ch = val.charAt(i);
        if (ch=='\'' || ch=='\\') {
          writer.write('\\');
        }
        writer.write(ch);
      }
    } else {
      writer.write(val);
    }
    writer.write('\'');
  }
}
