package org.apache.lucene.search.posfilter;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ExactPhraseQuery extends PositionFilterQuery {

  private final BooleanQuery innerBQ;

  public ExactPhraseQuery() {
    super(new BooleanQuery(), new ExactPhraseScorerFactory());
    this.innerBQ = (BooleanQuery) innerQuery;
  }

  public void add(Term term) {
    innerBQ.add(new TermQuery(term), BooleanClause.Occur.MUST);
  }

  private static class ExactPhraseScorerFactory implements ScorerFilterFactory {

    @Override
    public Scorer scorer(Scorer filteredScorer) {
      return new BlockPhraseScorer(filteredScorer);
    }

    @Override
    public String getName() {
      return "ExactPhrase";
    }
  }

}
