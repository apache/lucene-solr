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

package org.apache.lucene.luke.app.desktop.components.fragments.analysis;

import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.models.analysis.Analysis;

/** Operator of the Step by step analyze result panel */
public interface StepByStepAnalyzeResultPanelOperator
    extends ComponentOperatorRegistry.ComponentOperator {

  void setAnalysisModel(Analysis analysisModel);

  void executeAnalysisStepByStep(String text);

  void clearTable();
}
