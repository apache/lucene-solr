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
import java.util.function.IntSupplier;

/**
 * Abstract class to manage the extraction and writing of array data to a {@link DataOutput} stream.
 */
public abstract class ReductionDataArrayWriter<C> extends ReductionDataWriter<C> {
  private final IntSupplier sizeSupplier;

  public ReductionDataArrayWriter(DataOutput output, C extractor, IntSupplier sizeSupplier) {
    super(output, extractor);

    this.sizeSupplier = sizeSupplier;
  }

  /**
   * Write an array of data, retrieved from the extractor, and its size, received from the sizeSupplier, to the output stream.
   *
   * @throws IOException if an exception occurs while writing to the output stream
   */
  @Override
  public void write() throws IOException {
    int size = sizeSupplier.getAsInt();
    output.writeInt(size);
    write(size);
  }

  /**
   * Write an array of data, retrieved from the extractor, with the given size to the output stream.
   *
   * @throws IOException if an exception occurs while writing to the output stream
   */
  protected abstract void write(int size) throws IOException;
}