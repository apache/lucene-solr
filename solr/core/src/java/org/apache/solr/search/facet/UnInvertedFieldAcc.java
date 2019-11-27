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

/**
 * Base accumulator for {@link UnInvertedField}
 */
public abstract class UnInvertedFieldAcc extends SlotAcc implements UnInvertedField.Callback {

  UnInvertedField uif;
  UnInvertedField.DocToTerm docToTerm;

  public UnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
    super(fcontext);
    uif = UnInvertedField.getUnInvertedField(sf.getName(), fcontext.qcontext.searcher());
    docToTerm = uif.new DocToTerm();
    fcontext.qcontext.addCloseHook(this);
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }
}
