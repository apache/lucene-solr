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
import org.apache.lucene.search.Sort;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SortSpecParsing;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * A {@link MergePolicyFactory} for {@code SortingMergePolicy} objects.
 */
public class SortingMergePolicyFactory extends WrapperMergePolicyFactory {

  protected final Sort mergeSort;

  public SortingMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    final String sortArg = (String) args.remove(SORT);
    if (sortArg == null) {
      throw new IllegalArgumentException(SortingMergePolicyFactory.class.getSimpleName()+" requires a '"+ SORT + "' argument.");
    }
    this.mergeSort = SortSpecParsing.parseSortSpec(sortArg, schema).getSort();
  }

  @Override
  protected MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
    final MergePolicy mp = new SortingMergePolicy(wrappedMP, mergeSort);
    return mp;
  }

}
