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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.util.ContentStream;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

/**
 * A DataSource implementation which reads from the ContentStream of a POST request
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.4
 */
public class ContentStreamDataSource extends DataSource<Reader> {
  private ContextImpl context;
  private ContentStream contentStream;
  private Reader reader;

  @Override
  public void init(Context context, Properties initProps) {
    this.context = (ContextImpl) context;
  }

  @Override
  public Reader getData(String query) {
    contentStream = context.getDocBuilder().getReqParams().getContentStream();
    if (contentStream == null)
      throw new DataImportHandlerException(SEVERE, "No stream available. The request has no body");
    try {
      return reader = contentStream.getReader();
    } catch (IOException e) {
      DataImportHandlerException.wrapAndThrow(SEVERE, e);
      return null;
    }
  }

  @Override
  public void close() {
    if (contentStream != null) {
      try {
        if (reader == null) reader = contentStream.getReader();
        reader.close();
      } catch (IOException e) {
      }
    }
  }
}
