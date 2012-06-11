package org.apache.lucene.facet.index.categorypolicy;

import java.io.Serializable;

import org.apache.lucene.facet.index.streaming.CategoryParentsStream;
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
 * Filtering category paths in {@link CategoryParentsStream}, where a given
 * category is added to the stream, and than all its parents are being
 * added one after the other by successively removing the last component. <br>
 * That loop should have a stop point - the default approach (excluding the
 * ROOT) is implemented in {@link DefaultOrdinalPolicy}.
 * 
 * @lucene.experimental
 */
public interface PathPolicy extends Serializable {

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