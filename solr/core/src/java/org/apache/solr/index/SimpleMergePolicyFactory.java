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
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;

/**
 * A {@link MergePolicyFactory} for simple {@link MergePolicy} objects. Implementations need only create the policy
 * {@link #getMergePolicyInstance() instance} and this class will then configure it with all set properties.
 */
public abstract class SimpleMergePolicyFactory extends MergePolicyFactory {

  protected SimpleMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
  }

  protected abstract MergePolicy getMergePolicyInstance();

  @Override
  public final MergePolicy getMergePolicy() {
    final MergePolicy mp = getMergePolicyInstance();
    args.invokeSetters(mp);
    return mp;
  }

}
