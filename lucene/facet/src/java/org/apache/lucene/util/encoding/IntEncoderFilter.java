package org.apache.lucene.util.encoding;

import java.io.IOException;
import java.io.OutputStream;

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
 * An abstract implementation of {@link IntEncoder} which is served as a filter
 * on the values to encode. An encoder filter wraps another {@link IntEncoder}
 * which does the actual encoding. This allows for chaining filters and
 * encoders, such as: <code><pre>
 * new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEnoder()));
 * {@link UniqueValuesIntEncoder} followed by {@link DGapIntEncoder}
  </pre></code>
 * <p>
 * The default implementation implements {@link #close()} by closing the wrapped
 * encoder and {@link #reInit(OutputStream)} by re-initializing the wrapped
 * encoder.
 * 
 * @lucene.experimental
 */
public abstract class IntEncoderFilter extends IntEncoder {

  protected final IntEncoder encoder;

  protected IntEncoderFilter(IntEncoder encoder) {
    this.encoder = encoder;
  }

  @Override
  public void close() throws IOException {
    // There is no need to call super.close(), since we don't pass the output
    // stream to super.
    encoder.close();
  }

  @Override
  public void reInit(OutputStream out) {
    encoder.reInit(out);
  }

}
