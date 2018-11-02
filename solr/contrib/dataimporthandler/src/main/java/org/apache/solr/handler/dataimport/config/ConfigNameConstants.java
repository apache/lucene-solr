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
package org.apache.solr.handler.dataimport.config;

import org.apache.solr.handler.dataimport.SolrWriter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ConfigNameConstants {
  public static final String SCRIPT = "script";

  public static final String NAME = "name";

  public static final String PROCESSOR = "processor";
  
  public static final String PROPERTY_WRITER = "propertyWriter";

  public static final String IMPORTER_NS = "dataimporter";

  public static final String IMPORTER_NS_SHORT = "dih";

  public static final String ROOT_ENTITY = "rootEntity";
  
  public static final String CHILD = "child";

  public static final String FUNCTION = "function";

  public static final String CLASS = "class";

  public static final String DATA_SRC = "dataSource";

  public static final Set<String> RESERVED_WORDS;
  static{
    Set<String> rw =  new HashSet<>();
    rw.add(IMPORTER_NS);
    rw.add(IMPORTER_NS_SHORT);
    rw.add("request");
    rw.add("delta");
    rw.add("functions");
    rw.add("session");
    rw.add(SolrWriter.LAST_INDEX_KEY);
    RESERVED_WORDS = Collections.unmodifiableSet(rw);
  } 
}
