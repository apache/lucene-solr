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

import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.Version;
import org.apache.solr.core.Config;

/**
 * General token testing helper functions
 */
public abstract class BaseTokenTestCase extends BaseTokenStreamTestCase
{
  /** a map containing the default test version param for easy testing */
  protected static final Map<String,String> DEFAULT_VERSION_PARAM = 
    Collections.singletonMap("luceneMatchVersion", System.getProperty("tests.luceneMatchVersion", "LUCENE_CURRENT"));

  /** The default test version for easy testing */
  public static final Version DEFAULT_VERSION = Config.parseLuceneVersionString(DEFAULT_VERSION_PARAM.get("luceneMatchVersion"));
}
