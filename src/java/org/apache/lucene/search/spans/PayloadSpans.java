package org.apache.lucene.search.spans;
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

import java.util.Collection;
import java.io.IOException;


/**
 *   <font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental.
   * The APIs introduced here might change in the future and will not be
   * supported anymore in such a case.</font>
 *
 **/
public interface PayloadSpans extends Spans{
   /**
   * Returns the payload data for the current span.
   * This is invalid until {@link #next()} is called for
   * the first time.
   * This method must not be called more than once after each call
   * of {@link #next()}. However, payloads are loaded lazily,
   * so if the payload data for the current position is not needed,
   * this method may not be called at all for performance reasons.<br>
   * <br>
    * Note that the return type is a collection, thus the ordering should not be relied upon.
    * <br/>
   * <p><font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental.
   * The APIs introduced here might change in the future and will not be
   * supported anymore in such a case.</font>
   *
   * @return a List of byte arrays containing the data of this payload, otherwise null if isPayloadAvailable is false
   * @throws java.io.IOException
    */
  // TODO: Remove warning after API has been finalized
  Collection/*<byte[]>*/ getPayload() throws IOException;

  /**
   * Checks if a payload can be loaded at this position.
   * <p/>
   * Payloads can only be loaded once per call to
   * {@link #next()}.
   * <p/>
   * <p><font color="#FF0000">
   * WARNING: The status of the <b>Payloads</b> feature is experimental.
   * The APIs introduced here might change in the future and will not be
   * supported anymore in such a case.</font>
   *
   * @return true if there is a payload available at this position that can be loaded
   */
  // TODO: Remove warning after API has been finalized
  public boolean isPayloadAvailable();

}
