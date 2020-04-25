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
package org.apache.solr.analytics.stream.reservation;

import java.util.function.BooleanSupplier;

import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;

/**
 * A reservation allows a {@link ReductionDataCollector} to specify a piece of data it needs to export from the shard.
 * This data may, or may not, exist. The {@link ReductionDataCollector} need not check while importing/exporting since
 * the Reader/Writers handle all checking.
 */
public abstract class ReductionCheckedDataReservation<A, E> extends ReductionDataReservation<A, E> {
  protected final BooleanSupplier exists;

  protected ReductionCheckedDataReservation(A applier, E extractor, BooleanSupplier exists) {
    super(applier, extractor);
    this.exists = exists;
  }
}