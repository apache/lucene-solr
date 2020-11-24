package org.apache.lucene.search;

import java.io.IOException;
import java.util.List;

/** A Query that matches documents matching combinations of {@link TermQuery}s 
 * or other IndriAndQuerys.
 */
public class IndriAndQuery extends IndriQuery {

    public IndriAndQuery(List<BooleanClause> clauses) {
    	super(clauses);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
		IndriAndQuery query = this;
		return new IndriAndWeight(query, searcher, ScoreMode.TOP_SCORES, boost);
    }

}
