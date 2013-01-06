package org.apache.lucene.facet.taxonomy.writercache.cl2o;

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
 * Abstract class for storing Label->Ordinal mappings in a taxonomy. 
 * 
 * @lucene.experimental
 */
public abstract class LabelToOrdinal {

  protected int counter;
  public static final int INVALID_ORDINAL = -2;

  /**
   * return the maximal Ordinal assigned so far
   */
  public int getMaxOrdinal() {
    return this.counter;
  }

  /**
   * Returns the next unassigned ordinal. The default behavior of this method
   * is to simply increment a counter.
   */
  public int getNextOrdinal() {
    return this.counter++;
  }

  /**
   * Adds a new label if its not yet in the table.
   * Throws an {@link IllegalArgumentException} if the same label with
   * a different ordinal was previoulsy added to this table.
   */
  public abstract void addLabel(CategoryPath label, int ordinal);

  /**
   * @return the ordinal assigned to the given label, 
   * or {@link #INVALID_ORDINAL} if the label cannot be found in this table.
   */
  public abstract int getOrdinal(CategoryPath label);

}
