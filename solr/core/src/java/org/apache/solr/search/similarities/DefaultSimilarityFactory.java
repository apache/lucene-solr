package org.apache.solr.search.similarities;

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
import org.apache.lucene.search.similarities.Similarity;

import org.apache.solr.common.params.SolrParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @deprecated This class has been renamed to <code>ClassicSimilarityFactory</code> to reflect the renaming of the underlying Similarity returned.
 *
 * @see ClassicSimilarityFactory
 */
@Deprecated
public class DefaultSimilarityFactory extends ClassicSimilarityFactory {
  
  public static final Logger log = LoggerFactory.getLogger(DefaultSimilarityFactory.class);

  @Override
  public void init(SolrParams params) {
    super.init(params);
    log.warn("DefaultSimilarityFactory has been renamed and deprecated.  " +
             "Please update your configuration file to refer to ClassicSimilarityFactory instead");
  }
  
}
