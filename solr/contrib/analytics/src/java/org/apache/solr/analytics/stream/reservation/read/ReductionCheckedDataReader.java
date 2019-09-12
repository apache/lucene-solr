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

/**
 * Abstract class to manage the reading and application of data from a {@link DataInput} stream.
 * The data being read may not exist, so the reader first checks before reading.
 */
public abstract class ReductionCheckedDataReader<A> extends ReductionDataReader<A> {

  public ReductionCheckedDataReader(DataInput inputStream, A applier) {
    super(inputStream, applier);
  }

  @Override
  /**
   * Read a piece of data from the input stream and feed it to the applier.
   * <br>
   * First checks that the piece of data exists before reading.
   *
   * @throws IOException if an exception occurs while reading from the input stream
   */
  public void read() throws IOException {
    if (inputStream.readBoolean()) {
      checkedRead();
    }
  }

  /**
   * Read a piece of data from the input stream and feed it to the applier.
   * <br>
   * This piece of data is guaranteed to be there.
   *
   * @throws IOException if an exception occurs while reading from the input stream
   */
  protected abstract void checkedRead() throws IOException;
}