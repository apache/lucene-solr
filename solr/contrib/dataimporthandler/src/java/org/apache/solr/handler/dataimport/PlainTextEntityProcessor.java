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

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.XPathEntityProcessor.URL;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>An implementation of {@link EntityProcessor} which reads data from a url/file and give out a row which contains one String
 * value. The name of the field is 'plainText'.
 *
 * @since solr 1.4
 */
public class PlainTextEntityProcessor extends EntityProcessorBase {
  private boolean ended = false;

  @Override
  public void init(Context context) {
    super.init(context);
    ended = false;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  @Override
  public Map<String, Object> nextRow() {
    if (ended) return null;
    DataSource<Reader> ds = context.getDataSource();
    String url = context.replaceTokens(context.getEntityAttribute(URL));
    Reader r = null;
    try {
      r = ds.getData(url);
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e, "Exception reading url : " + url);
    }
    StringWriter sw = new StringWriter();
    char[] buf = new char[1024];
    while (true) {
      int len = 0;
      try {
        len = r.read(buf);
      } catch (IOException e) {
        IOUtils.closeQuietly(r);
        wrapAndThrow(SEVERE, e, "Exception reading url : " + url);
      }
      if (len <= 0) break;
      sw.append(new String(buf, 0, len));
    }
    Map<String, Object> row = new HashMap<>();
    row.put(PLAIN_TEXT, sw.toString());
    ended = true;
    IOUtils.closeQuietly(r);
    return row;
  }

  public static final String PLAIN_TEXT = "plainText";
}
