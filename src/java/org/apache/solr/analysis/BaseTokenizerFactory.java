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

import org.apache.solr.core.Config;
import org.apache.solr.schema.IndexSchema;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.util.Version;


/**
 * Simple abstract implementation that handles init arg processing.
 * 
 * @version $Id$
 */
public abstract class BaseTokenizerFactory implements TokenizerFactory {
  public static final Logger log = LoggerFactory.getLogger(BaseTokenizerFactory.class);
  
  /** The init args */
  protected Map<String,String> args;
  
  /** the luceneVersion arg */
  protected Version luceneMatchVersion = null;

  public void init(Map<String,String> args) {
    this.args=args;
    String matchVersion = args.get(IndexSchema.LUCENE_MATCH_VERSION_PARAM);
    if (matchVersion != null) {
      luceneMatchVersion = Config.parseLuceneVersionString(matchVersion);
    }
  }
  
  public Map<String,String> getArgs() {
    return args;
  }
}
