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

package org.apache.solr.core;

/**
 * A MultiCore singleton.
 * Marked as deprecated to avoid usage proliferation of core code that would
 * assume MultiCore being a singleton.  In solr 2.0, the MultiCore factory
 * should be popluated with a standard tool like spring.  Until then, this is
 * a simple static factory that should not be used widely. 
 * 
 * @version $Id$
 * @since solr 1.3
 */
@Deprecated
public final class SolrMultiCore extends MultiCore
{
  private static MultiCore instance = null;
  
  // no one else can make the registry
  private SolrMultiCore() {}
  
  /** Returns a default MultiCore singleton.
   * @return
   */
  public static synchronized MultiCore getInstance() {
    if (instance == null) {
      instance = new SolrMultiCore();
    }
    return instance;
  }
}
