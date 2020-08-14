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

import org.apache.solr.schema.SchemaField;

public class UniqueBlockFieldAgg extends UniqueBlockAgg {

  public UniqueBlockFieldAgg(String field) {
    super(field);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    final String fieldName = getArg();
    SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(fieldName);
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      throw new IllegalArgumentException(name+"("+fieldName+
          ") doesn't allow multivalue fields, got " + sf);
    } else {
      if (sf.getType().getNumberType() != null) {
        throw new IllegalArgumentException(name+"("+fieldName+
            ") not yet support numbers " + sf);
      } else {
        return new UniqueBlockSlotAcc(fcontext, sf, numSlots);
      }
    }
  }
}
