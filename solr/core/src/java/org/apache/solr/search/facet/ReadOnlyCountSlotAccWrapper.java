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
package org.apache.solr.search.facet;

import java.io.IOException;

final class ReadOnlyCountSlotAccWrapper extends ReadOnlyCountSlotAcc {

  private final CountSlotAcc backing;

  ReadOnlyCountSlotAccWrapper(FacetContext fcontext, CountSlotAcc backing) {
    super(fcontext);
    this.backing = backing;
  }

  @Override
  public int getCount(int slot) {
    return backing.getCount(slot);
  }

  @Override
  public int compare(int slotA, int slotB) {
    return backing.compare(slotA, slotB);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return backing.getValue(slotNum);
  }
}
