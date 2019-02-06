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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.FastWriter;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Static utility methods relating to {@link QueryResponseWriter}s
 */
public final class QueryResponseWriterUtil {
  private QueryResponseWriterUtil() { /* static helpers only */ }

  /**
   * Writes the response writer's result to the given output stream.
   * This method inspects the specified writer to determine if it is a 
   * {@link BinaryQueryResponseWriter} or not to delegate to the appropriate method.
   * @see BinaryQueryResponseWriter#write(OutputStream,SolrQueryRequest,SolrQueryResponse)
   * @see BinaryQueryResponseWriter#write(Writer,SolrQueryRequest,SolrQueryResponse)
   */
  public static void writeQueryResponse(OutputStream outputStream,
      QueryResponseWriter responseWriter, SolrQueryRequest solrRequest,
      SolrQueryResponse solrResponse, String contentType) throws IOException {
    
    if (responseWriter instanceof BinaryQueryResponseWriter) {
      BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
      binWriter.write(outputStream, solrRequest, solrResponse);
    } else {
      OutputStream out = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          outputStream.write(b);
        }
        @Override
        public void flush() throws IOException {
          // We don't flush here, which allows us to flush below
          // and only flush internal buffers, not the response.
          // If we flush the response early, we trigger chunked encoding.
          // See SOLR-8669.
        }
      };
      Writer writer = buildWriter(out, ContentStreamBase.getCharsetFromContentType(contentType));
      responseWriter.write(writer, solrRequest, solrResponse);
      writer.flush();
    }
  }
  
  private static Writer buildWriter(OutputStream outputStream, String charset) throws UnsupportedEncodingException {
    Writer writer = (charset == null) ? new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        : new OutputStreamWriter(outputStream, charset);
    
    return new FastWriter(writer);
  }
}
