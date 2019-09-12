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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;

/** {@link UnInvertedField} implementation of field faceting.
 * It's a top-level term cache. */
class FacetFieldProcessorByArrayUIF extends FacetFieldProcessorByArray {
  UnInvertedField uif;
  TermsEnum te;

  FacetFieldProcessorByArrayUIF(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
    if (! sf.isUninvertible()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              getClass()+" can not be used on fields where uninvertible='false'");
    }
  }

  @Override
  protected void findStartAndEndOrds() throws IOException {
    uif = UnInvertedField.getUnInvertedField(freq.field, fcontext.searcher);
    te = uif.getOrdTermsEnum( fcontext.searcher.getSlowAtomicReader() );    // "te" can be null

    startTermIndex = 0;
    endTermIndex = uif.numTerms();  // one past the end

    if (prefixRef != null && te != null) {
      if (te.seekCeil(prefixRef.get()) == TermsEnum.SeekStatus.END) {
        startTermIndex = uif.numTerms();
      } else {
        startTermIndex = (int) te.ord();
      }
      prefixRef.append(UnicodeUtil.BIG_TERM);
      if (te.seekCeil(prefixRef.get()) == TermsEnum.SeekStatus.END) {
        endTermIndex = uif.numTerms();
      } else {
        endTermIndex = (int) te.ord();
      }
    }

    nTerms = endTermIndex - startTermIndex;
  }

  @Override
  protected void collectDocs() throws IOException {
    uif.collectDocs(this);
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return uif.getTermValue(te, ord);
  }
}
