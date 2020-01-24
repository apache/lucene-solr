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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

/** A simple update request which streams content to the server
 */
public class StreamingUpdateRequest extends AbstractUpdateRequest {

  private final RequestWriter.ContentWriter contentWriter;

  public StreamingUpdateRequest(String path, RequestWriter.ContentWriter contentWriter) {
    super(METHOD.POST, path);
    this.contentWriter = contentWriter;
  }

  public StreamingUpdateRequest(String path, String content, String contentType) {
    this(path, new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        os.write(content.getBytes(StandardCharsets.UTF_8));
      }

      @Override
      public String getContentType() {
        return contentType;
      }

    });
  }

  public StreamingUpdateRequest(String path, File f, String contentType) {
    this(path, new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        try (InputStream is = new FileInputStream(f)) {
          IOUtils.copy(is, os);
        }
      }

      @Override
      public String getContentType() {
        return contentType;
      }
    });
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return contentWriter;
  }

}
