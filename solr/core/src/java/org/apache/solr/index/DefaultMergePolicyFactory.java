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
package org.apache.solr.index;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;

/**
 * A {@link MergePolicyFactory} for the default {@link MergePolicy}.
 */
public class DefaultMergePolicyFactory extends MergePolicyFactory {

  public DefaultMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were "+args+" but "+getClass().getSimpleName()+" takes no arguments.");
    }
  }

  public DefaultMergePolicyFactory() {
    super(null, null, null);
  }

  @Override
  public final MergePolicy getMergePolicy() {
    return new TieredMergePolicy();
  }

}
