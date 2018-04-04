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
package org.apache.solr.search;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean.Category;
import org.apache.solr.search.SolrCache.State;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Common base class of reusable functionality for SolrCaches
 */
public abstract class SolrCacheBase {
   
  protected CacheRegenerator regenerator;
  
  private State state;
  
  private String name;
  
  protected AutoWarmCountRef autowarm;
  
  /**
   * Decides how many things to autowarm based on the size of another cache
   */
  public static class AutoWarmCountRef {

    private final int autoWarmCount;
    private final int autoWarmPercentage;
    private final boolean autoWarmByPercentage;
    private final boolean doAutoWarming;
    private final String strVal;
    public AutoWarmCountRef(final String configValue) {
      try {
        String input = (null == configValue) ? "0" : configValue.trim();

        // odd undocumented legacy behavior, -1 meant "all" (now "100%")
        strVal = ("-1".equals(input)) ? "100%" : input;

        if (strVal.indexOf("%") == (strVal.length() - 1)) {
          autoWarmCount = 0;
          autoWarmPercentage = Integer.parseInt(strVal.substring(0, strVal.length() - 1));
          autoWarmByPercentage = true;
          doAutoWarming = (0 < autoWarmPercentage);
        } else {
          autoWarmCount = Integer.parseInt(strVal);
          autoWarmPercentage = 0;
          autoWarmByPercentage = false;
          doAutoWarming = (0 < autoWarmCount);
        }

      } catch (Exception e) {
        throw new RuntimeException("Can't parse autoWarm value: " + configValue, e);
      }
    }
    @Override
    public String toString() {
      return strVal;
    }
    public boolean isAutoWarmingOn() {
      return doAutoWarming;
    }
    public int getWarmCount(final int previousCacheSize) {
      return autoWarmByPercentage ? 
        (previousCacheSize * autoWarmPercentage)/100 :
        Math.min(previousCacheSize, autoWarmCount);
    }
  }

  /**
   * Returns a "Hit Ratio" (ie: max of 1.00, not a percentage) suitable for 
   * display purposes.
   */
  protected static float calcHitRatio(long lookups, long hits) {
    return (lookups == 0) ? 0.0f :
        BigDecimal.valueOf((double) hits / (double) lookups)
            .setScale(2, RoundingMode.HALF_EVEN)
            .floatValue();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public Category getCategory() {
    return Category.CACHE;
  }

  public void init(Map<String, String> args, CacheRegenerator regenerator) {
    this.regenerator = regenerator;
    state = State.CREATED;
    name = args.get(NAME);
    autowarm = new AutoWarmCountRef(args.get("autowarmCount"));
    
  }
  
  protected String getAutowarmDescription() {
    return "autowarmCount=" + autowarm + ", regenerator=" + regenerator;
  }
  
  protected boolean isAutowarmingOn() {
    return autowarm.isAutoWarmingOn();
  }
  
  public void setState(State state) {
    this.state = state;
  }

  public State getState() {
    return state;
  }
  
  public String name() {
    return this.name;
  }

}

