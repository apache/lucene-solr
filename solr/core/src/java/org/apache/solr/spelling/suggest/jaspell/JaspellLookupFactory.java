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
package org.apache.solr.spelling.suggest.jaspell;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.jaspell.JaspellLookup;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.LookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link JaspellLookup}
 * <b>Note:</b> This Suggester is not very RAM efficient.
 */
public class JaspellLookupFactory extends LookupFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String FILENAME = "jaspell.dat";

  @Override
  public Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core) {
    log.info("init: {}", params);
    return new JaspellLookup();
  }

  @Override
  public String storeFileName() {
    return FILENAME;
  }
}
