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
 * <p>
 * This package contains the main logic for performing the reranking using
 * a <b>LTR model</b>.
 * </p>
 * <p>
 * A LTR model is plugged into the ranking through the {@link org.apache.solr.ltr.ranking.LTRQParserPlugin},
 * a {@link org.apache.solr.search.QParserPlugin}. The plugin will
 * read from the request the model (instance of {@link org.apache.solr.ltr.ranking.ModelQuery})
 * used to perform the request plus other
 * parameters. The plugin will generate a {@link org.apache.solr.ltr.ranking.LTRQuery}:
 * a particular {@link org.apache.solr.search.RankQuery}
 * that will encapsulate the given model and use it to
 * rescore and rerank the document (by using an {@link org.apache.solr.ltr.ranking.LTRCollector}).
 * </p>
 * <p>
 * A model will be applied on each document through a {@link org.apache.solr.ltr.ranking.ModelQuery}, a
 * subclass of {@link org.apache.lucene.search.Query}. As a normal query,
 * the learned model will produce a new score
 * for each document reranked.
 * </p>
 * <p>
 * A {@link org.apache.solr.ltr.ranking.ModelQuery} is created by providing an instance of
 * {@link org.apache.solr.ltr.feature.ModelMetadata}. An instance of
 * {@link org.apache.solr.ltr.feature.ModelMetadata}
 * defines how to combine the features in order to create a new
 * score for a document. A new learning to rank model is plugged
 * into the framework  by extending {@link org.apache.solr.ltr.feature.ModelMetadata},
 * (see for example {@link org.apache.solr.ltr.ranking.LambdaMARTModel} and {@link org.apache.solr.ltr.ranking.RankSVMModel}).
 * </p>
 * <p>
 * The {@link org.apache.solr.ltr.ranking.ModelQuery} will take care of computing the values of
 * all the features (see {@link org.apache.solr.ltr.ranking.Feature}) and then will delegate the final score
 * generation to the {@link org.apache.solr.ltr.feature.ModelMetadata}, by calling the method
 * {@link org.apache.solr.ltr.feature.ModelMetadata#score(float[] modelFeatureValuesNormalized) score(float[] modelFeatureValuesNormalized)}.
 * </p>
 * <p>
 * Finally, a {@link org.apache.solr.ltr.ranking.Feature} will produce a particular value for each document, so
 * it is modeled as a {@link org.apache.lucene.search.Query}. The package <i>org.apache.solr.ltr.feature.impl</i> contains several examples
 * of features. One benefit of extending the Query object is that we can reuse
 * Query as a feature, see for example {@link org.apache.solr.ltr.feature.impl.SolrFeature}.
 *
 *
 *
 *
 *
 */
package org.apache.solr.ltr.ranking;
