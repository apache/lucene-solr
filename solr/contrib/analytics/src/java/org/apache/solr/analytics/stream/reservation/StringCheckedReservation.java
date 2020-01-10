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
import java.util.function.Supplier;

import org.apache.solr.analytics.stream.reservation.read.StringCheckedDataReader;
import org.apache.solr.analytics.stream.reservation.write.StringCheckedDataWriter;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class StringCheckedReservation extends ReductionCheckedDataReservation<Consumer<String>, Supplier<String>> {

  public StringCheckedReservation(Consumer<String> applier, Supplier<String> extractor, BooleanSupplier exists) {
    super(applier, extractor, exists);
  }

  @Override
  public StringCheckedDataReader createReadStream(DataInput input) {
    return new StringCheckedDataReader(input, applier);
  }

  @Override
  public StringCheckedDataWriter createWriteStream(DataOutput output) {
    return new StringCheckedDataWriter(output, extractor, exists);
  }
}