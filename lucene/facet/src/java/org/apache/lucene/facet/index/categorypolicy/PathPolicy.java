package org.apache.lucene.facet.index.categorypolicy;

import java.io.Serializable;

import org.apache.lucene.facet.index.DrillDownStream;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/**
 * Determines which {@link CategoryPath categories} should be added as terms to
 * the {@link DrillDownStream}. The default approach is implemented by
 * {@link #ALL_CATEGORIES}.
 * 
 * @lucene.experimental
 */
public interface PathPolicy extends Serializable {

  /**
   * A {@link PathPolicy} which adds all {@link CategoryPath} that have at least
   * one component (i.e. {@link CategoryPath#length} &gt; 0) to the categories
   * stream.
   */
  public static final PathPolicy ALL_CATEGORIES = new PathPolicy() {
    @Override
    public boolean shouldAdd(CategoryPath categoryPath) { return categoryPath.length > 0; }
  };
  
  /**
   * Check whether a given category path should be added to the stream.
   * 
   * @param categoryPath
   *            A given category path which is to be tested for stream
   *            addition.
   * @return <code>true</code> if the category path should be added.
   *         <code>false</code> otherwise.
   */
  public abstract boolean shouldAdd(CategoryPath categoryPath);

}