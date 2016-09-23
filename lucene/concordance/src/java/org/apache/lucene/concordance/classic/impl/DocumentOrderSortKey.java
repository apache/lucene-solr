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

package org.apache.lucene.concordance.classic.impl;

import org.apache.lucene.concordance.classic.ConcordanceSortKey;

/**
 * This sorts based alphabetically on the document key
 * and then numerically on the
 */
public class DocumentOrderSortKey extends ConcordanceSortKey {

  protected final int targetCharStart;

  public DocumentOrderSortKey(String docKey, int targetCharStart) {
    super(docKey);
    this.targetCharStart = targetCharStart;
  }

  @Override
  public int compareTo(ConcordanceSortKey o) {
    if (o instanceof DocumentOrderSortKey) {
      DocumentOrderSortKey other = (DocumentOrderSortKey) o;
      int cmp = super.compareTo(o);
      if (cmp == 0) {
        return Integer.compare(targetCharStart, other.targetCharStart);
      }
      return cmp;
    } else {
      return super.compareTo(o);
    }
  }
}
