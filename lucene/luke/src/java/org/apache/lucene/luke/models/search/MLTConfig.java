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

package org.apache.lucene.luke.models.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.queries.mlt.MoreLikeThis;

/**
 * Configurations for MoreLikeThis query.
 */
public final class MLTConfig {

  private final List<String> fields;

  private final int maxDocFreq;

  private final int minDocFreq;

  private final int minTermFreq;

  /** Builder for {@link MLTConfig} */
  public static class Builder {

    private final List<String> fields = new ArrayList<>();
    private int maxDocFreq = MoreLikeThis.DEFAULT_MAX_DOC_FREQ;
    private int minDocFreq = MoreLikeThis.DEFAULT_MIN_DOC_FREQ;
    private int minTermFreq = MoreLikeThis.DEFAULT_MIN_TERM_FREQ;

    public Builder fields(Collection<String> val) {
      fields.addAll(val);
      return this;
    }

    public Builder maxDocFreq(int val) {
      maxDocFreq = val;
      return this;
    }

    public Builder minDocFreq(int val) {
      minDocFreq = val;
      return this;
    }

    public Builder minTermFreq(int val) {
      minTermFreq = val;
      return this;
    }

    public MLTConfig build() {
      return new MLTConfig(this);
    }
  }

  private MLTConfig(Builder builder) {
    this.fields = Collections.unmodifiableList(builder.fields);
    this.maxDocFreq = builder.maxDocFreq;
    this.minDocFreq = builder.minDocFreq;
    this.minTermFreq = builder.minTermFreq;
  }

  public String[] getFieldNames() {
    return fields.toArray(new String[fields.size()]);
  }

  public int getMaxDocFreq() {
    return maxDocFreq;
  }

  public int getMinDocFreq() {
    return minDocFreq;
  }

  public int getMinTermFreq() {
    return minTermFreq;
  }

}
