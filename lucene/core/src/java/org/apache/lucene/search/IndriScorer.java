package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * The Indri parent scorer that stores the boost so that 
 * IndriScorers can use the boost outside of the term.
 *
 */
abstract public class IndriScorer extends Scorer {

	private float boost;

	protected IndriScorer(Weight weight, float boost) {
		super(weight);
		this.boost = boost;
	}

	@Override
	abstract public DocIdSetIterator iterator();

	@Override
	abstract public float getMaxScore(int upTo) throws IOException;

	@Override
	abstract public float score() throws IOException;

	abstract public float smoothingScore(int docId) throws IOException;

	@Override
	abstract public int docID();

	public float getBoost() {
		return this.boost;
	}

}
