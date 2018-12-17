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

/**
 * This package contains the various ranking models that can be used in Lucene. The
 * abstract class {@link org.apache.lucene.search.similarities.Similarity} serves
 * as the base for ranking functions. For searching, users can employ the models
 * already implemented or create their own by extending one of the classes in this
 * package.
 * 
 * <h2>Table Of Contents</h2>
 *     <ol>
 *         <li><a href="#sims">Summary of the Ranking Methods</a></li>
 *         <li><a href="#changingSimilarity">Changing the Similarity</a></li>
 *     </ol>
 * 
 * 
 * <a name="sims"></a>
 * <h2>Summary of the Ranking Methods</h2>
 * 
 * <p>{@link org.apache.lucene.search.similarities.BM25Similarity} is an optimized
 * implementation of the successful Okapi BM25 model.
 *
 * <p>{@link org.apache.lucene.search.similarities.ClassicSimilarity} is the original Lucene
 * scoring function. It is based on the
 * <a href="http://en.wikipedia.org/wiki/Vector_Space_Model">Vector Space Model</a>. For more
 * information, see {@link org.apache.lucene.search.similarities.TFIDFSimilarity}.
 * 
 * <p>{@link org.apache.lucene.search.similarities.SimilarityBase} provides a basic
 * implementation of the Similarity contract and exposes a highly simplified
 * interface, which makes it an ideal starting point for new ranking functions.
 * Lucene ships the following methods built on
 * {@link org.apache.lucene.search.similarities.SimilarityBase}:
 * 
 * <a name="framework"></a>
 * <ul>
 *   <li>Amati and Rijsbergen's {@linkplain org.apache.lucene.search.similarities.DFRSimilarity DFR} framework;</li>
 *   <li>Clinchant and Gaussier's {@linkplain org.apache.lucene.search.similarities.IBSimilarity Information-based models}
 *     for IR;</li>
 *   <li>The implementation of two {@linkplain org.apache.lucene.search.similarities.LMSimilarity language models} from
 *   Zhai and Lafferty's paper.</li>
 *   <li>{@linkplain org.apache.lucene.search.similarities.DFISimilarity Divergence from independence} models as described
 *   in "IRRA at TREC 2012" (Dinçer).
 *   <li>
 * </ul>
 * 
 * Since {@link org.apache.lucene.search.similarities.SimilarityBase} is not
 * optimized to the same extent as
 * {@link org.apache.lucene.search.similarities.ClassicSimilarity} and
 * {@link org.apache.lucene.search.similarities.BM25Similarity}, a difference in
 * performance is to be expected when using the methods listed above. However,
 * optimizations can always be implemented in subclasses; see
 * <a href="#changingSimilarity">below</a>.
 * 
 * <a name="changingSimilarity"></a>
 * <h2>Changing Similarity</h2>
 * 
 * <p>Chances are the available Similarities are sufficient for all
 *     your searching needs.
 *     However, in some applications it may be necessary to customize your <a
 *         href="Similarity.html">Similarity</a> implementation. For instance, some
 *     applications do not need to distinguish between shorter and longer documents
 *     and could set BM25's {@link org.apache.lucene.search.similarities.BM25Similarity#BM25Similarity(float,float) b}
 *     parameter to {@code 0}.
 * 
 * <p>To change {@link org.apache.lucene.search.similarities.Similarity}, one must do so for both indexing and
 *     searching, and the changes must happen before
 *     either of these actions take place. Although in theory there is nothing stopping you from changing mid-stream, it
 *     just isn't well-defined what is going to happen.
 * 
 * <p>To make this change, implement your own {@link org.apache.lucene.search.similarities.Similarity} (likely
 *     you'll want to simply subclass {@link org.apache.lucene.search.similarities.SimilarityBase}), and
 *     then register the new class by calling
 *     {@link org.apache.lucene.index.IndexWriterConfig#setSimilarity(Similarity)}
 *     before indexing and
 *     {@link org.apache.lucene.search.IndexSearcher#setSimilarity(Similarity)}
 *     before searching.
 * 
 * <h3>Tuning {@linkplain org.apache.lucene.search.similarities.BM25Similarity}</h3>
 * <p>{@link org.apache.lucene.search.similarities.BM25Similarity} has
 * two parameters that may be tuned:
 * <ul>
 *   <li><tt>k1</tt>, which calibrates term frequency saturation and must be
 *   positive or null. A value of {@code 0} makes term frequency completely
 *   ignored, making documents scored only based on the value of the <tt>IDF</tt>
 *   of the matched terms. Higher values of <tt>k1</tt> increase the impact of
 *   term frequency on the final score. Default value is {@code 1.2}.</li>
 *   <li><tt>b</tt>, which controls how much document length should normalize
 *   term frequency values and must be in {@code [0, 1]}. A value of {@code 0}
 *   disables length normalization completely. Default value is {@code 0.75}.</li>
 * </ul> 
 *
 * <h3>Extending {@linkplain org.apache.lucene.search.similarities.SimilarityBase}</h3>
 * <p>
 * The easiest way to quickly implement a new ranking method is to extend
 * {@link org.apache.lucene.search.similarities.SimilarityBase}, which provides
 * basic implementations for the low level . Subclasses are only required to
 * implement the {@link org.apache.lucene.search.similarities.SimilarityBase#score(BasicStats, double, double)}
 * and {@link org.apache.lucene.search.similarities.SimilarityBase#toString()}
 * methods.
 * 
 * <p>Another option is to extend one of the <a href="#framework">frameworks</a>
 * based on {@link org.apache.lucene.search.similarities.SimilarityBase}. These
 * Similarities are implemented modularly, e.g.
 * {@link org.apache.lucene.search.similarities.DFRSimilarity} delegates
 * computation of the three parts of its formula to the classes
 * {@link org.apache.lucene.search.similarities.BasicModel},
 * {@link org.apache.lucene.search.similarities.AfterEffect} and
 * {@link org.apache.lucene.search.similarities.Normalization}. Instead of
 * subclassing the Similarity, one can simply introduce a new basic model and tell
 * {@link org.apache.lucene.search.similarities.DFRSimilarity} to use it.
 * 
 */
package org.apache.lucene.search.similarities;
