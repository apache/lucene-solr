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
package org.apache.solr.analytics.stream.reservation.write;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.BooleanSupplier;

/**
 * Abstract class to manage the extraction and writing of data to a {@link DataOutput} stream.
 * The data being written may not exist, so the writer first writes whether the data exists before writing the data.
 */
public abstract class ReductionCheckedDataWriter<C> extends ReductionDataWriter<C> {
  private final BooleanSupplier existsSupplier;

  public ReductionCheckedDataWriter(DataOutput output, C extractor, BooleanSupplier existsSupplier) {
    super(output, extractor);

    this.existsSupplier = existsSupplier;
  }

  /**
   * Write a piece of data, retrieved from the extractor, to the output stream.
   * <br>
   * First writes whether the data exists, then if it does exists writes the data.
   *
   * @throws IOException if an exception occurs while writing to the output stream
   */
  @Override
  public void write() throws IOException {
    boolean exists = existsSupplier.getAsBoolean();
    output.writeBoolean(exists);
    if (exists) {
      checkedWrite();
    }
  }

  /**
   * Write a piece of data, retrieved from the extractor, to the output stream.
   * <br>
   * The data being written is guaranteed to exist.
   *
   * @throws IOException if an exception occurs while writing to the output stream
   */
  protected abstract void checkedWrite() throws IOException;
}