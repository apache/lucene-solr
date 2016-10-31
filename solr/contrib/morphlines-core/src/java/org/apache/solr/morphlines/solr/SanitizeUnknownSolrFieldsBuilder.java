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
package org.apache.solr.morphlines.solr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.solr.schema.IndexSchema;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * Command that sanitizes record fields that are unknown to Solr schema.xml by either deleting them
 * (renameToPrefix is absent or a zero length string), or by moving them to a field prefixed with
 * the given renameToPrefix (e.g. renameToPrefix = "ignored_" to use typical dynamic Solr fields).
 * <p>
 * Recall that Solr throws an exception on any attempt to load a document that contains a field that
 * isn't specified in schema.xml.
 */
public final class SanitizeUnknownSolrFieldsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("sanitizeUnknownSolrFields");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new SanitizeUnknownSolrFields(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SanitizeUnknownSolrFields extends AbstractCommand {
    
    private final IndexSchema schema;
    private final String renameToPrefix;
        
    public SanitizeUnknownSolrFields(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      
      Config solrLocatorConfig = getConfigs().getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      this.schema = Objects.requireNonNull(locator.getIndexSchema());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Solr schema: \n" +
            schema.getFields().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue).map(Object::toString).collect(Collectors.joining("\n"))
        );
      }

      String str = getConfigs().getString(config, "renameToPrefix", "").trim();
      this.renameToPrefix = str.length() > 0 ? str : null;  
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record record) {
      Collection<Map.Entry> entries = new ArrayList<Map.Entry>(record.getFields().asMap().entrySet());
      for (Map.Entry<String, Collection<Object>> entry : entries) {
        String key = entry.getKey();
        if (schema.getFieldOrNull(key) == null) {
          LOG.debug("Sanitizing unknown Solr field: {}", key);
          Collection values = entry.getValue();
          if (renameToPrefix != null) {
            record.getFields().putAll(renameToPrefix + key, values);
          }
          values.clear(); // implicitly removes key from record
        }
      }
      return super.doProcess(record);
    }
    
  }
}
