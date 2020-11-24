package org.apache.lucene.search;

import java.io.IOException;
import java.util.List;

/**
 * Combines scores of subscorers.  If a subscorer does not contain the docId,
 * a smoothing score is calculated for that document/subscorer combination.
 *
 */
public class IndriAndScorer extends IndriDisjunctionScorer {

	protected IndriAndScorer(Weight weight, List<Scorer> subScorers, ScoreMode scoreMode, float boost)
			throws IOException {
		super(weight, subScorers, scoreMode, boost);
	}

	@Override
	public float score(List<Scorer> subScorers) throws IOException {
		int docId = this.docID();
		return scoreDoc(subScorers, docId);
	}

	@Override
	public float smoothingScore(List<Scorer> subScorers, int docId) throws IOException {
		return scoreDoc(subScorers, docId);
	}

	private float scoreDoc(List<Scorer> subScorers, int docId) throws IOException {
		double score = 0;
		double boostSum = 0.0;
		for (Scorer scorer : subScorers) {
			if (scorer instanceof IndriScorer) {
				IndriScorer indriScorer = (IndriScorer) scorer;
				int scorerDocId = indriScorer.docID();
				if (docId == scorerDocId) {
					double tempScore = indriScorer.score();
					tempScore *= indriScorer.getBoost();
					score += tempScore;
				} else {
					float smoothingScore = indriScorer.smoothingScore(docId);
					smoothingScore *= indriScorer.getBoost();
					score += smoothingScore;
				}
				boostSum += indriScorer.getBoost();
			}
		}
		if (boostSum == 0) {
			return 0;
		} else {
			return (float) (score / boostSum);
		}
	}

}
