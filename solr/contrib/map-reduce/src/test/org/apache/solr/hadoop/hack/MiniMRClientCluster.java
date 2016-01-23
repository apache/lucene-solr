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
package org.apache.solr.hadoop.hack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/*
 * A simple interface for a client MR cluster used for testing. This interface
 * provides basic methods which are independent of the underlying Mini Cluster (
 * either through MR1 or MR2).
 */
public interface MiniMRClientCluster {

  public void start() throws IOException;

  /**
   * Stop and start back the cluster using the same configuration.
   */
  public void restart() throws IOException;

  public void stop() throws IOException;

  public Configuration getConfig() throws IOException;

}
