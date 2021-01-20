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

package org.apache.solr.client.solrj.request;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.params.UpdateParams.ASSUME_CONTENT_TYPE;

public class MultiContentWriterRequest extends AbstractUpdateRequest {

  @SuppressWarnings({"rawtypes"})
  private final Iterator<Pair<NamedList, Object>> payload;

  /**
   *
   * @param m HTTP method
   * @param path path to which to post to
   * @param payload add the per doc params, The Object could be a ByteBuffer or byte[]
   */

  public MultiContentWriterRequest(METHOD m, String path,
                                   @SuppressWarnings({"rawtypes"})Iterator<Pair<NamedList, Object>> payload) {
    super(m, path);
    params = new ModifiableSolrParams();
    params.add("multistream", "true");
    this.payload = payload;
  }


  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return new RequestWriter.ContentWriter() {
      @Override
      @SuppressWarnings({"unchecked"})
      public void write(OutputStream os) throws IOException {
        new JavaBinCodec().marshal((IteratorWriter) iw -> {
          while (payload.hasNext()) {
            @SuppressWarnings({"rawtypes"})
            Pair<NamedList, Object> next = payload.next();

            if (next.second() instanceof ByteBuffer || next.second() instanceof byte[]) {
              @SuppressWarnings({"rawtypes"})
              NamedList params = next.first();
              if(params.get(ASSUME_CONTENT_TYPE) == null){
                String detectedType = detect(next.second());
                if(detectedType==null){
                  throw new RuntimeException("Unknown content type");
                }
                params.add(ASSUME_CONTENT_TYPE, detectedType);
              }
              iw.add(params);
              iw.add(next.second());
            }  else {
              throw new RuntimeException("payload value must be byte[] or ByteBuffer");
            }
          }
        }, os);
      }

      @Override
      public String getContentType() {
        return "application/javabin";
      }
    };
  }
  public static String detect(Object o) throws IOException {
    Reader rdr = null;
    byte[] bytes = null;
    if (o instanceof byte[]) bytes = (byte[]) o;
    else if (o instanceof ByteBuffer) bytes = ((ByteBuffer) o).array();
    rdr = new InputStreamReader(new ByteArrayInputStream(bytes), UTF_8);
    String detectedContentType = null;
    for (;;) {
      int ch = rdr.read();
      if (Character.isWhitespace(ch)) {
        continue;
      }
      int nextChar = -1;
      // first non-whitespace chars
      if (ch == '#'                         // single line comment
          || (ch == '/' && ((nextChar = rdr.read()) == '/' || nextChar == '*'))  // single line or multi-line comment
          || (ch == '{' || ch == '[')       // start of JSON object
          )
      {
        detectedContentType = "application/json";
      } else if (ch == '<') {
        detectedContentType = "text/xml";
      }
      break;
    }
    return detectedContentType;
  }

  public static ByteBuffer readByteBuffer(InputStream is) throws IOException {
    BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
    org.apache.commons.io.IOUtils.copy(is, baos);
    return ByteBuffer.wrap(baos.getbuf(), 0, baos.size());
  }
}
