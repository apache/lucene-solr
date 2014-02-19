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

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.typesafe.config.Config;

/**
 * A command that assigns a record unique key that is the concatenation of the given
 * <code>baseIdField</code> record field, followed by a running count of the record number within
 * the current session. The count is reset to zero whenever a "startSession" notification is
 * received.
 * <p>
 * For example, assume a CSV file containing multiple records but no unique ids, and the
 * <code>baseIdField</code> field is the filesystem path of the file. Now this command can be used
 * to assign the following record values to Solr's unique key field:
 * <code>$path#0, $path#1, ... $path#N</code>.
 * <p>
 * The name of the unique key field is fetched from Solr's schema.xml file, as directed by the
 * <code>solrLocator</code> configuration parameter.
 */
public final class GenerateSolrSequenceKeyBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Arrays.asList(
        "generateSolrSequenceKey", 
        "sanitizeUniqueSolrKey" // old name (retained for backwards compatibility)
    );
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new GenerateSolrSequenceKey(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class GenerateSolrSequenceKey extends AbstractCommand {
    
    private final boolean preserveExisting;
    private final String baseIdFieldName;
    private final String uniqueKeyName;
    private long recordCounter = 0;
  
    private final String idPrefix; // for load testing only; enables adding same document many times with a different unique key
    private final Random randomIdPrefix; // for load testing only; enables adding same document many times with a different unique key

    public GenerateSolrSequenceKey(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.baseIdFieldName = getConfigs().getString(config, "baseIdField", Fields.BASE_ID);
      this.preserveExisting = getConfigs().getBoolean(config, "preserveExisting", true);      
      
      Config solrLocatorConfig = getConfigs().getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      IndexSchema schema = locator.getIndexSchema();
      SchemaField uniqueKey = schema.getUniqueKeyField();
      uniqueKeyName = uniqueKey == null ? null : uniqueKey.getName();
      
      String tmpIdPrefix = getConfigs().getString(config, "idPrefix", null);  // for load testing only
      Random tmpRandomIdPrefx = null;
      if ("random".equals(tmpIdPrefix)) { // for load testing only
        tmpRandomIdPrefx = new Random(new SecureRandom().nextLong());    
        tmpIdPrefix = null;
      }
      idPrefix = tmpIdPrefix;
      randomIdPrefix = tmpRandomIdPrefx;
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record doc) {      
      long num = recordCounter++;
      // LOG.debug("record #{} id before sanitizing doc: {}", num, doc);
      if (uniqueKeyName == null || (preserveExisting && doc.getFields().containsKey(uniqueKeyName))) {
        ; // we must preserve the existing id
      } else {
        Object baseId = doc.getFirstValue(baseIdFieldName);
        if (baseId == null) {
          throw new MorphlineRuntimeException("Record field " + baseIdFieldName
              + " must not be null as it is needed as a basis for a unique key for solr doc: " + doc);
        }
        doc.replaceValues(uniqueKeyName, baseId.toString() + "#" + num);          
      }
      
      // for load testing only; enables adding same document many times with a different unique key
      if (idPrefix != null) { 
        String id = doc.getFirstValue(uniqueKeyName).toString();
        id = idPrefix + id;
        doc.replaceValues(uniqueKeyName, id);
      } else if (randomIdPrefix != null) {
        String id = doc.getFirstValue(uniqueKeyName).toString();
        id = String.valueOf(Math.abs(randomIdPrefix.nextInt())) + "#" + id;
        doc.replaceValues(uniqueKeyName, id);
      }

      LOG.debug("record #{} unique key sanitized to this: {}", num, doc);
      
      return super.doProcess(doc);
    }
    
    @Override
    protected void doNotify(Record notification) {
      if (Notifications.containsLifecycleEvent(notification, Notifications.LifecycleEvent.START_SESSION)) {
        recordCounter = 0; // reset
      }
      super.doNotify(notification);
    }

  }
}
