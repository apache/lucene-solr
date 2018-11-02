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
 * a Learning to Rank model.
 * </p>
 * <p>
 * A model will be applied on each document through a {@link org.apache.solr.ltr.LTRScoringQuery}, a
 * subclass of {@link org.apache.lucene.search.Query}. As a normal query,
 * the learned model will produce a new score
 * for each document reranked.
 * </p>
 * <p>
 * A {@link org.apache.solr.ltr.LTRScoringQuery} is created by providing an instance of
 * {@link org.apache.solr.ltr.model.LTRScoringModel}. An instance of
 * {@link org.apache.solr.ltr.model.LTRScoringModel}
 * defines how to combine the features in order to create a new
 * score for a document. A new Learning to Rank model is plugged
 * into the framework  by extending {@link org.apache.solr.ltr.model.LTRScoringModel},
 * (see for example {@link org.apache.solr.ltr.model.MultipleAdditiveTreesModel} and {@link org.apache.solr.ltr.model.LinearModel}).
 * </p>
 * <p>
 * The {@link org.apache.solr.ltr.LTRScoringQuery} will take care of computing the values of
 * all the features (see {@link org.apache.solr.ltr.feature.Feature}) and then will delegate the final score
 * generation to the {@link org.apache.solr.ltr.model.LTRScoringModel}, by calling the method
 * {@link org.apache.solr.ltr.model.LTRScoringModel#score(float[] modelFeatureValuesNormalized) score(float[] modelFeatureValuesNormalized)}.
 * </p>
 */
package org.apache.solr.ltr;
