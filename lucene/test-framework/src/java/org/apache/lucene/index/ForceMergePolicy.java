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
package org.apache.lucene.index;

import java.io.IOException;

/**
 * A {@link MergePolicy} that only returns forced merges.
 * <p>
 * <b>NOTE</b>: Use this policy if you wish to disallow background merges but wish to run optimize/forceMerge segment
 * merges.
 *
 * @lucene.experimental
 */
public final class ForceMergePolicy extends FilterMergePolicy {

  /** Create a new {@code ForceMergePolicy} around the given {@code MergePolicy} */
  public ForceMergePolicy(MergePolicy in) {
    super(in);
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    return null;
  }

}
