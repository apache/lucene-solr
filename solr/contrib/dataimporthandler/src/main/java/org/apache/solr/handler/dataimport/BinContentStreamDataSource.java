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

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
/**
 * <p> A data source implementation which can be used to read binary stream from content streams. </p> <p> Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a> for more
 * details. </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 3.1
 */

public class BinContentStreamDataSource extends DataSource<InputStream> {
  private ContextImpl context;
  private ContentStream contentStream;
  private InputStream in;


  @Override
  public void init(Context context, Properties initProps) {
    this.context = (ContextImpl) context;
  }

  @Override
  public InputStream getData(String query) {
     contentStream = context.getDocBuilder().getReqParams().getContentStream();
    if (contentStream == null)
      throw new DataImportHandlerException(SEVERE, "No stream available. The request has no body");
    try {
      return in = contentStream.getStream();
    } catch (IOException e) {
      DataImportHandlerException.wrapAndThrow(SEVERE, e);
      return null;
    }
  }

  @Override
  public void close() {
     if (contentStream != null) {
      try {
        if (in == null) in = contentStream.getStream();
        in.close();
      } catch (IOException e) {
        /*no op*/
      }
    } 
  }
}
