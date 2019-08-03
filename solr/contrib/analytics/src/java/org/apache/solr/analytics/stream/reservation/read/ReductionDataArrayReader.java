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
package org.apache.solr.analytics.stream.reservation.read;

import java.io.DataInput;
import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * Abstract class to manage the reading and application of array data from a {@link DataInput} stream.
 */
public abstract class ReductionDataArrayReader<A> extends ReductionDataReader<A> {
  protected final IntConsumer signal;

  public ReductionDataArrayReader(DataInput inputStream, A applier, IntConsumer signal) {
    super(inputStream, applier);

    this.signal = signal;
  }

  @Override
  /**
   * Read an array of data from the input stream and feed it to the applier, first signaling the size of the array.
   *
   * @throws IOException if an exception occurs while reading from the input stream
   */
  public void read() throws IOException {
    int size = inputStream.readInt();
    signal.accept(size);
    read(size);
  }

  /**
   * Read an array from the input stream, feeding each member to the applier.
   *
   * @param size length of the array to read
   * @throws IOException if an exception occurs while reading from the input stream
   */
  protected abstract void read(int size) throws IOException;
}