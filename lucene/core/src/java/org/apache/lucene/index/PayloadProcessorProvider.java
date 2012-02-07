package org.apache.lucene.index;

/**
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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Provides a {@link ReaderPayloadProcessor} to be used for a {@link Directory}.
 * This allows using different {@link ReaderPayloadProcessor}s for different
 * source {@link AtomicReader}, for e.g. to perform different processing of payloads of
 * different directories.
 * <p>
 * <b>NOTE:</b> to avoid processing payloads of certain directories, you can
 * return <code>null</code> in {@link #getReaderProcessor}.
 * <p>
 * <b>NOTE:</b> it is possible that the same {@link ReaderPayloadProcessor} will be
 * requested for the same {@link Directory} concurrently. Therefore, to avoid
 * concurrency issues you should return different instances for different
 * threads. Usually, if your {@link ReaderPayloadProcessor} does not maintain state
 * this is not a problem. The merge code ensures that the
 * {@link ReaderPayloadProcessor} instance you return will be accessed by one
 * thread to obtain the {@link PayloadProcessor}s for different terms.
 * 
 * @lucene.experimental
 */
public abstract class PayloadProcessorProvider {

  /**
   * Returns a {@link PayloadProcessor} for a given {@link Term} which allows
   * processing the payloads of different terms differently. If you intent to
   * process all your payloads the same way, then you can ignore the given term.
   * <p>
   * <b>NOTE:</b> if you protect your {@link ReaderPayloadProcessor} from
   * concurrency issues, then you shouldn't worry about any such issues when
   * {@link PayloadProcessor}s are requested for different terms.
   */
  public static abstract class ReaderPayloadProcessor {

    /** Returns a {@link PayloadProcessor} for the given term. */
    public abstract PayloadProcessor getProcessor(String field, BytesRef text) throws IOException;
    
  }

  /**
   * Processes the given payload.
   * 
   * @lucene.experimental
   */
  public static abstract class PayloadProcessor {

    /** Process the incoming payload and stores the result in the given {@link BytesRef}. */
    public abstract void processPayload(BytesRef payload) throws IOException;

  }

  /**
   * Returns a {@link ReaderPayloadProcessor} for the given {@link Directory},
   * through which {@link PayloadProcessor}s can be obtained for each
   * {@link Term}, or <code>null</code> if none should be used.
   */
  public abstract ReaderPayloadProcessor getReaderProcessor(AtomicReader reader) throws IOException;

}
