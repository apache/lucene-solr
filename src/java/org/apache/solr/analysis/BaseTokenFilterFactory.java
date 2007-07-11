/**
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

package org.apache.solr.analysis;


import java.util.Map;

/**
 * Simple abstract implementation that handles init arg processing.
 * 
 * @version $Id$
 */
public abstract class BaseTokenFilterFactory implements TokenFilterFactory {
  /** The init args */
  protected Map<String,String> args;
  public void init(Map<String,String> args) {
    this.args=args;
  }

  public Map<String,String> getArgs() {
    return args;
  }

  // TODO: move these somewhere that tokenizers and others
  // can also use them...
  protected int getInt(String name) {
    return getInt(name,-1,false);
  }

  protected int getInt(String name, int defaultVal) {
    return getInt(name,defaultVal,true);
  }

  protected int getInt(String name, int defaultVal, boolean useDefault) {
    String s = args.get(name);
    if (s==null) {
      if (useDefault) return defaultVal;
      throw new RuntimeException("Configuration Error: missing parameter '" + name + "'");
    }
    return Integer.parseInt(s);
  }

  protected boolean getBoolean(String name, boolean defaultVal) {
    return getBoolean(name,defaultVal,true);
  }

  protected boolean getBoolean(String name, boolean defaultVal, boolean useDefault) {
    String s = args.get(name);
    if (s==null) {
      if (useDefault) return defaultVal;
      throw new RuntimeException("Configuration Error: missing parameter '" + name + "'");
    }
    return Boolean.parseBoolean(s);
  }

}
