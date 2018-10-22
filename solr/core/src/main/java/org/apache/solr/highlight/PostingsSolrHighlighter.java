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
package org.apache.solr.highlight;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Highlighter impl that uses {@link UnifiedHighlighter} configured to operate as it's ancestor/predecessor, the
 * {code PostingsHighlighter}.
 *
 * @deprecated Use {@link UnifiedSolrHighlighter} instead
 */
@Deprecated
public class PostingsSolrHighlighter extends UnifiedSolrHighlighter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void init(PluginInfo info) {
    log.warn("The PostingsSolrHighlighter is deprecated; use the UnifiedSolrHighlighter instead.");
    super.init(info);
  }

  @Override
  protected UnifiedHighlighter getHighlighter(SolrQueryRequest req) {
    // Adjust the highlight parameters to match what the old PostingsHighlighter had.
    ModifiableSolrParams invariants = new ModifiableSolrParams();
    invariants.set(HighlightParams.OFFSET_SOURCE, "POSTINGS");
    invariants.set(HighlightParams.FIELD_MATCH, true);
    invariants.set(HighlightParams.USE_PHRASE_HIGHLIGHTER, false);
    invariants.set(HighlightParams.FRAGSIZE, -1);

    ModifiableSolrParams defaults = new ModifiableSolrParams();
    defaults.set(HighlightParams.DEFAULT_SUMMARY, true);
    defaults.set(HighlightParams.TAG_ELLIPSIS, "... ");

    SolrParams newParams = SolrParams.wrapDefaults(
        invariants,// this takes precedence
        SolrParams.wrapDefaults(
            req.getParams(), // then this (original)
            defaults // finally our defaults
        )
    );
    try (LocalSolrQueryRequest fakeReq = new LocalSolrQueryRequest(req.getCore(), newParams)) {
      return super.getHighlighter(fakeReq);
    }
  }
}
