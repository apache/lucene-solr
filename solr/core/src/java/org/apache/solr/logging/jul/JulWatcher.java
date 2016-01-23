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
package org.apache.solr.logging.jul;

import com.google.common.base.Throwables;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.logging.CircularList;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LoggerInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

@SuppressForbidden(reason = "class is specific to java.util.logging")
public class JulWatcher extends LogWatcher<LogRecord> {

  final String name;
  RecordHandler handler = null;
  
  public JulWatcher(String name) {
    this.name = name;
  }
  
  @Override
  public String getName() {
    return "JUL ("+name+")";
  }


  @Override
  public List<String> getAllLevels() {
    return Arrays.asList(
      Level.FINEST.getName(),
      Level.FINER.getName(),
      Level.FINE.getName(),
      Level.CONFIG.getName(),
      Level.INFO.getName(),
      Level.WARNING.getName(),
      Level.SEVERE.getName(),
      Level.OFF.getName() );
  }

  @Override
  public void setLogLevel(String category, String level) {
    if(LoggerInfo.ROOT_NAME.equals(category)) {
      category = "";
    }
    
    Logger log = LogManager.getLogManager().getLogger(category);
    if(level==null||"unset".equals(level)||"null".equals(level)) {
      if(log!=null) {
        log.setLevel(null);
      }
    }
    else {
      if(log==null) {
        log = Logger.getLogger(category); // create it
      }
      log.setLevel(Level.parse(level));
    }
  }

  @Override
  public Collection<LoggerInfo> getAllLoggers() {
    LogManager manager = LogManager.getLogManager();

    Logger root = manager.getLogger("");
    Map<String,LoggerInfo> map = new HashMap<>();
    Enumeration<String> names = manager.getLoggerNames();
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      Logger logger = Logger.getLogger(name);
      if( logger == root) {
        continue;
      }
      map.put(name, new JulInfo(name, logger));

      while (true) {
        int dot = name.lastIndexOf(".");
        if (dot < 0)
          break;
        name = name.substring(0, dot);
        if(!map.containsKey(name)) {
          map.put(name, new JulInfo(name, null));
        }
      }
    }
    map.put(LoggerInfo.ROOT_NAME, new JulInfo(LoggerInfo.ROOT_NAME, root));
    return map.values();
  }

  @Override
  public void setThreshold(String level) {
    if(handler==null) {
      throw new IllegalStateException("Must have an handler");
    }
    handler.setLevel( Level.parse(level) );
  }

  @Override
  public String getThreshold() {
    if(handler==null) {
      throw new IllegalStateException("Must have an handler");
    }
    return handler.getLevel().toString();
  }

  @Override
  public void registerListener(ListenerConfig cfg) {
    if(history!=null) {
      throw new IllegalStateException("History already registered");
    }
    history = new CircularList<>(cfg.size);
    handler = new RecordHandler(this);
    if(cfg.threshold != null) {
      handler.setLevel(Level.parse(cfg.threshold));
    }
    else {
      handler.setLevel(Level.WARNING);
    }
    
    Logger log = LogManager.getLogManager().getLogger("");
    log.addHandler(handler);
  }

  @Override
  public long getTimestamp(LogRecord event) {
    return event.getMillis();
  }

  @Override
  public SolrDocument toSolrDocument(LogRecord event) {
    SolrDocument doc = new SolrDocument();
    doc.setField("time", new Date(event.getMillis()));
    doc.setField("level", event.getLevel().toString());
    doc.setField("logger", event.getLoggerName());
    doc.setField("message", event.getMessage().toString());
    Throwable t = event.getThrown();
    if(t!=null) {
      doc.setField("trace", Throwables.getStackTraceAsString(t));
    }
    return doc;
  }
}