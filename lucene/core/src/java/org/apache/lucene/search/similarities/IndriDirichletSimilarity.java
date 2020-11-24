/*
 * ===============================================================================================
 * Copyright (c) 2019 Carnegie Mellon University and University of Massachusetts. All Rights
 * Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval is subject to the terms
 * of the software license set forth in the LICENSE file included with this software, and also
 * available at http://www.lemurproject.org/license.html
 *
 * ================================================================================================
 */
package org.apache.lucene.search.similarities;

import java.util.List;
import java.util.Locale;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.LMSimilarity;

/**
 * Bayesian smoothing using Dirichlet priors as implemented in the Indri Search
 * engine (http://www.lemurproject.org/indri.php).  
 * Indri Dirichelet Smoothing!
 *                 tf_E + mu*P(t|D)
 * P(t|E)= ------------------------
 *          documentLength + documentMu
 *                  mu*P(t|C) + tf_D
 * where P(t|D)= ---------------------
 *                  doclen + mu
 */
public class IndriDirichletSimilarity extends LMSimilarity {

	/** The &mu; parameter. */
	private final float mu;

	/** Instantiates the similarity with the provided &mu; parameter. */
	public IndriDirichletSimilarity(CollectionModel collectionModel, float mu) {
		super(collectionModel);
		this.mu = mu;
	}

	/** Instantiates the similarity with the provided &mu; parameter. */
	public IndriDirichletSimilarity(float mu) {
		this.mu = mu;
	}

	/** Instantiates the similarity with the default &mu; value of 2000. */
	public IndriDirichletSimilarity(CollectionModel collectionModel) {
		this(collectionModel, 2000);
	}

	/** Instantiates the similarity with the default &mu; value of 2000. */
	public IndriDirichletSimilarity() {
		this(new IndriCollectionModel(), 2000);
	}

	@Override
	protected double score(BasicStats stats, double freq, double docLen) {
		double collectionProbability = ((LMStats) stats).getCollectionProbability();
		double score = (freq + (mu * collectionProbability)) / (docLen + mu);
		return (Math.log(score));
	}

	@Override
	protected void explain(List<Explanation> subs, BasicStats stats, double freq, double docLen) {
		if (stats.getBoost() != 1.0f) {
			subs.add(Explanation.match(stats.getBoost(), "boost"));
		}

		subs.add(Explanation.match(mu, "mu"));
		double collectionProbability = ((LMStats) stats).getCollectionProbability();
		Explanation weightExpl = Explanation
				.match((float) Math.log((freq + (mu * collectionProbability)) / (docLen + mu)), "term weight");
		subs.add(weightExpl);
		subs.add(Explanation.match((float) Math.log(mu / (docLen + mu)), "document norm"));
		super.explain(subs, stats, freq, docLen);
	}

	/** Returns the &mu; parameter. */
	public float getMu() {
		return mu;
	}

	public String getName() {
		return String.format(Locale.ROOT, "IndriDirichlet(%f)", getMu());
	}

	/**
	 * Models {@code p(w|C)} as the number of occurrences of the term in the
	 * collection, divided by the total number of tokens {@code + 1}.
	 */
	public static class IndriCollectionModel implements CollectionModel {

		/** Sole constructor: parameter-free */
		public IndriCollectionModel() {
		}

		@Override
		public double computeProbability(BasicStats stats) {
			return ((double) stats.getTotalTermFreq()) / ((double) stats.getNumberOfFieldTokens());
		}

		@Override
		public String getName() {
			return null;
		}
	}

}
