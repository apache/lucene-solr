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

import java.io.Serializable;
import java.net.URL;
import java.util.Map;
 
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean.Category;
import org.apache.solr.search.SolrCache.State;

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
  protected static String calcHitRatio(long lookups, long hits) {
    if (lookups==0) return "0.00";
    if (lookups==hits) return "1.00";
    int hundredths = (int)(hits*100/lookups);   // rounded down
    if (hundredths < 10) return "0.0" + hundredths;
    return "0." + hundredths;

    /*** code to produce a percent, if we want it...
    int ones = (int)(hits*100 / lookups);
    int tenths = (int)(hits*1000 / lookups) - ones*10;
    return Integer.toString(ones) + '.' + tenths;
    ***/
  }


  public String getVersion() {
    return SolrCore.version;
  }

  public Category getCategory() {
    return Category.CACHE;
  }

  public URL[] getDocs() {
    return null;
  }
  
  public void init(Map<String, String> args, CacheRegenerator regenerator) {
    this.regenerator = regenerator;
    state=State.CREATED;
    name = (String) args.get("name");
    autowarm = new AutoWarmCountRef((String)args.get("autowarmCount"));
    
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

