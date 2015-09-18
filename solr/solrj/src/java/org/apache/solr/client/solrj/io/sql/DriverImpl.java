package org.apache.solr.client.solrj.io.sql;

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


import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.solr.common.util.SuppressForbidden;

/**
 *  Get a Connection with with a url and properties.
 *
 *  jdbc:solr://zkhost:port?collection=collection&amp;aggregationMode=map_reduce
 **/


public class DriverImpl implements Driver {

  static {
    try {
      DriverManager.registerDriver(new DriverImpl());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!", e);
    }
  }

  public Connection connect(String url, Properties props) throws SQLException {
    if(!acceptsURL(url)) {
      return null;
    }

    StringBuilder buf = new StringBuilder(url);
    boolean needsAmp = true;
    if(!url.contains("?")) {
      buf.append("?");
      needsAmp = false;
    }

    for(Object key : props.keySet()) {
      Object value = props.get(key);
      if(needsAmp) {
        buf.append("&");
      }
      buf.append(key.toString()).append("=").append(value);
      needsAmp = true;
    }

    return connect(buf.toString());
  }

  public Connection connect(String url) throws SQLException {

    if(!acceptsURL(url)) {
      return null;
    }

    String[] parts = url.split("://", 0);

    if(parts.length < 2) {
      throw new SQLException("The zkHost must start with ://");
    }

    String zkUrl  = parts[1];
    String[] zkUrlParts = zkUrl.split("\\?");

    if(zkUrlParts.length < 2) {
      throw new SQLException("The connection url has no connection properties. At a mininum the collection must be specified.");
    }

    String connectionProps = zkUrlParts[1];
    String zkHost = zkUrlParts[0];
    Properties props = new Properties();
    loadParams(connectionProps, props);
    String collection = (String)props.remove("collection");

    if(!props.containsKey("aggregationMode")) {
      props.setProperty("aggregationMode","facet");
    }

    return new ConnectionImpl(zkHost, collection, props);
  }

  public int getMajorVersion() {
    return 1;
  }

  public int getMinorVersion() {
    return 0;
  }

  public boolean acceptsURL(String url) {
    if(url.startsWith("jdbc:solr")) {
      return true;
    } else {
      return false;
    }
  }

  public boolean jdbcCompliant() {
    return false;
  }


  @SuppressForbidden(reason="Required by jdbc")

  public Logger getParentLogger() {
    return null;
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return null;
  }

  private void loadParams(String params, Properties props) throws SQLException {
    try {
      String[] pairs = params.split("&");
      for (String pair : pairs) {
        String[] keyValue = pair.split("=");
        String key = URLDecoder.decode(keyValue[0], "UTF-8");
        String value = URLDecoder.decode(keyValue[1], "UTF-8");
        props.put(key, value);
      }
    } catch(Exception e) {
      throw new SQLException(e);
    }
  }
}