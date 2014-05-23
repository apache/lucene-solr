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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A command that loads a record into a SolrServer or MapReduce SolrOutputFormat.
 */
public final class LoadSolrBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("loadSolr");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new LoadSolr(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class LoadSolr extends AbstractCommand {
    
    private final DocumentLoader loader;
    private final Map<String, Float> boosts = new HashMap();
    private final Timer elapsedTime;    
    
    public LoadSolr(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      Config solrLocatorConfig = getConfigs().getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      this.loader = locator.getLoader();
      Config boostsConfig = getConfigs().getConfig(config, "boosts", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(boostsConfig)) {
        String fieldName = entry.getKey();        
        float boost = Float.parseFloat(entry.getValue().toString().trim());
        boosts.put(fieldName, boost);
      }
      validateArguments();
      this.elapsedTime = getTimer(Metrics.ELAPSED_TIME);
    }

    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.BEGIN_TRANSACTION) {
          try {
            loader.beginTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifecycleEvent.COMMIT_TRANSACTION) {
          try {
            loader.commitTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifecycleEvent.ROLLBACK_TRANSACTION) {
          try {
            loader.rollbackTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          try {
            loader.shutdown();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
      }
      super.doNotify(notification);
    }
    
    @Override
    protected boolean doProcess(Record record) {
      Timer.Context timerContext = elapsedTime.time();
      SolrInputDocument doc = convert(record);
      try {
        loader.load(doc);
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } catch (SolrServerException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        timerContext.stop();
      }
      
      // pass record to next command in chain:      
      return super.doProcess(record);
    }
    
    private SolrInputDocument convert(Record record) {
      Map<String, Collection<Object>> map = record.getFields().asMap();
      SolrInputDocument doc = new SolrInputDocument(new HashMap(2 * map.size()));
      for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
        String key = entry.getKey();
        doc.setField(key, entry.getValue(), getBoost(key));
      }
      return doc;
    }

    private float getBoost(String key) {
      if (boosts.size() > 0) {
        Float boost = boosts.get(key);
        if (boost != null) {
          return boost.floatValue();
        }
      }
      return 1.0f;
    }
    
  }
}
