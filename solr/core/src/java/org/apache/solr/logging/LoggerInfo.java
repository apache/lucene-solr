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
package org.apache.solr.logging;

import org.apache.solr.common.util.SimpleOrderedMap;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Wrapper class for Logger implementations
 */
public abstract class LoggerInfo implements Comparable<LoggerInfo> {
  public static final String ROOT_NAME = "root";

  protected final String name;
  protected String level;

  public LoggerInfo(String name) {
    this.name = name;
  }

  public String getLevel() {
    return level;
  }

  public String getName() {
    return name;
  }
  
  public abstract boolean isSet();

  public SimpleOrderedMap<?> getInfo() {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    info.add(NAME, getName());
    info.add("level", getLevel());
    info.add("set", isSet());
    return info;
  }

  @Override
  public int compareTo(LoggerInfo other) {
    if (this.equals(other))
      return 0;

    String tN = this.getName();
    String oN = other.getName();

    if(ROOT_NAME.equals(tN))
      return -1;
    if(ROOT_NAME.equals(oN))
      return 1;

    return tN.compareTo(oN);
  }
}