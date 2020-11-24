package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;

/**
 * The Weight for IndriAndQuery, used to
 * normalize, score and explain these queries.
 */
public class IndriAndWeight extends Weight {

	private final IndriAndQuery query;
	private final ArrayList<Weight> weights;
	private final ScoreMode scoreMode;
	private final float boost;

	public IndriAndWeight(IndriAndQuery query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
			throws IOException {
		super(query);
		this.query = query;
		this.boost = boost;
		this.scoreMode = scoreMode;
		weights = new ArrayList<>();
		for (BooleanClause c : query) {
			Weight w = searcher.createWeight(c.getQuery(), scoreMode, 1.0f);
			weights.add(w);
		}
	}

	private Scorer getScorer(LeafReaderContext context) throws IOException {
		List<Scorer> subScorers = new ArrayList<>();

		for (Weight w : weights) {
			Scorer scorer = w.scorer(context);
			if (scorer != null) {
				subScorers.add(scorer);
			}
		}

		if (subScorers.isEmpty()) {
			return null;
		}
		Scorer scorer = subScorers.get(0);
		if (subScorers.size() > 1) {
			scorer = new IndriAndScorer(this, subScorers, scoreMode, boost);
		}
		return scorer;
	}

	@Override
	public Scorer scorer(LeafReaderContext context) throws IOException {
		return getScorer(context);
	}

	@Override
	public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
		Scorer scorer = getScorer(context);
		if (scorer != null) {
			BulkScorer bulkScorer = new DefaultBulkScorer(scorer);
			return bulkScorer;
		}
		return null;
	}

	@Override
	public boolean isCacheable(LeafReaderContext ctx) {
		for (Weight w : weights) {
		    if (w.isCacheable(ctx) == false)
			return false;
		}
		return true;
	}

	@Override
	public Explanation explain(LeafReaderContext context, int doc) throws IOException {
		List<Explanation> subs = new ArrayList<>();
		boolean fail = false;
		Iterator<BooleanClause> cIter = query.iterator();
		for (Iterator<Weight> wIter = weights.iterator(); wIter.hasNext();) {
			Weight w = wIter.next();
			BooleanClause c = cIter.next();
			Explanation e = w.explain(context, doc);
			if (e.isMatch()) {
				subs.add(e);
			} else if (c.isRequired()) {
				subs.add(Explanation.noMatch("no match on required clause (" + c.getQuery().toString() + ")", e));
				fail = true;
			}
		}
		if (fail) {
			return Explanation.noMatch("Failure to meet condition(s) of required/prohibited clause(s)", subs);
		} else {
			Scorer scorer = scorer(context);
			int advanced = scorer.iterator().advance(doc);
			assert advanced == doc;
			return Explanation.match(scorer.score(), "sum of:", subs);
		}
	}

}
