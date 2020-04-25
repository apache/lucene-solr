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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;


/**
 * Basic functionality to upload a File or {@link org.apache.solr.common.util.ContentStream} to a Solr Cell or some
 * other handler that takes ContentStreams (CSV)
 * <p>
 * See http://wiki.apache.org/solr/ExtractingRequestHandler<br>
 * See http://wiki.apache.org/solr/UpdateCSV
 * 
 *
 **/
public class ContentStreamUpdateRequest extends AbstractUpdateRequest {
  List<ContentStream> contentStreams;

  /**
   *
   * @param url The URL to send the {@link org.apache.solr.common.util.ContentStream} to in Solr.
   */
  public ContentStreamUpdateRequest(String url) {
    super(METHOD.POST, url);
    contentStreams = new ArrayList<>();
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return contentStreams;
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    if (contentStreams == null || contentStreams.isEmpty() || contentStreams.size() > 1) return null;
    ContentStream stream = contentStreams.get(0);
    return new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        try(var inStream = stream.getStream()) {
          IOUtils.copy(inStream, os);
        }
      }

      @Override
      public String getContentType() {
        return stream.getContentType();
      }
    };
  }

  /**
   * Add a File to the {@link org.apache.solr.common.util.ContentStream}s.
   * @param file The File to add.
   * @throws IOException if there was an error with the file.
   *
   * @see #getContentStreams()
   * @see org.apache.solr.common.util.ContentStreamBase.FileStream
   */
  public void addFile(File file, String contentType) throws IOException {
    ContentStreamBase cs = new ContentStreamBase.FileStream(file);
    cs.setContentType(contentType);
    addContentStream(cs);
  }

  /**
   * Add a {@link org.apache.solr.common.util.ContentStream} to {@link #getContentStreams()}
   * @param contentStream The {@link org.apache.solr.common.util.ContentStream}
   */
  public void addContentStream(ContentStream contentStream){
    contentStreams.add(contentStream);
  }
  
}
