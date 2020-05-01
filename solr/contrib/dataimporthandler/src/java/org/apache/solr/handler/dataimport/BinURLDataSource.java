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

import static org.apache.solr.handler.dataimport.DataImportHandlerException.*;
import static org.apache.solr.handler.dataimport.URLDataSource.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
/**
 * <p> A data source implementation which can be used to read binary streams using HTTP. </p> <p> Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a> for more
 * details. </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 3.1
 */
public class BinURLDataSource extends DataSource<InputStream>{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String baseUrl;
  private int connectionTimeout = CONNECTION_TIMEOUT;

  private int readTimeout = READ_TIMEOUT;

  private Context context;

  private Properties initProps;

  public BinURLDataSource() { }

  @Override
  public void init(Context context, Properties initProps) {
      this.context = context;
    this.initProps = initProps;

    baseUrl = getInitPropWithReplacements(BASE_URL);
    String cTimeout = getInitPropWithReplacements(CONNECTION_TIMEOUT_FIELD_NAME);
    String rTimeout = getInitPropWithReplacements(READ_TIMEOUT_FIELD_NAME);
    if (cTimeout != null) {
      try {
        connectionTimeout = Integer.parseInt(cTimeout);
      } catch (NumberFormatException e) {
        log.warn("Invalid connection timeout: {}", cTimeout);
      }
    }
    if (rTimeout != null) {
      try {
        readTimeout = Integer.parseInt(rTimeout);
      } catch (NumberFormatException e) {
        log.warn("Invalid read timeout: {}", rTimeout);
      }
    }
  }

  @Override
  public InputStream getData(String query) {
    URL url = null;
    try {
      if (URIMETHOD.matcher(query).find()) url = new URL(query);
      else url = new URL(baseUrl + query);
      log.debug("Accessing URL: {}", url);
      URLConnection conn = url.openConnection();
      conn.setConnectTimeout(connectionTimeout);
      conn.setReadTimeout(readTimeout);
      return conn.getInputStream();
    } catch (Exception e) {
      log.error("Exception thrown while getting data", e);
      wrapAndThrow (SEVERE, e, "Exception in invoking url " + url);
      return null;//unreachable
    }
  }

  @Override
  public void close() { }

  private String getInitPropWithReplacements(String propertyName) {
    final String expr = initProps.getProperty(propertyName);
    if (expr == null) {
      return null;
    }
    return context.replaceTokens(expr);
  }
}
