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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.BooleanSupplier;

import org.apache.solr.analytics.stream.reservation.read.FloatCheckedDataReader;
import org.apache.solr.analytics.stream.reservation.write.FloatCheckedDataWriter;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.util.function.FloatSupplier;

public class FloatCheckedReservation extends ReductionCheckedDataReservation<FloatConsumer, FloatSupplier> {

  public FloatCheckedReservation(FloatConsumer applier, FloatSupplier extractor, BooleanSupplier exists) {
    super(applier, extractor, exists);
  }

  @Override
  public FloatCheckedDataReader createReadStream(DataInput input) {
    return new FloatCheckedDataReader(input, applier);
  }

  @Override
  public FloatCheckedDataWriter createWriteStream(DataOutput output) {
    return new FloatCheckedDataWriter(output, extractor, exists);
  }
}