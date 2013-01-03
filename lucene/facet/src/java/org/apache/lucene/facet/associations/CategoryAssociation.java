package org.apache.lucene.facet.associations;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

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
 * Allows associating an arbitrary value with a {@link CategoryPath}.
 * 
 * @lucene.experimental
 */
public interface CategoryAssociation {
  
  /** Serializes the associated value into the given {@link DataOutput}. */
  public void serialize(ByteArrayDataOutput output);

  /** Deserializes the association value from the given {@link DataInput}. */
  public void deserialize(ByteArrayDataInput input);
  
  /** Returns the maximum bytes needed to encode the association value. */
  public int maxBytesNeeded();
  
  /**
   * Returns the ID of the category association. The ID is used as e.g. the
   * term's text under which to encode the association values.
   */
  public String getCategoryListID();
  
}
