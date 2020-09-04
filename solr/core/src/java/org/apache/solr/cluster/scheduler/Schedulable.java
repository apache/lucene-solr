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
package org.apache.solr.cluster.scheduler;

/**
 * Component to be scheduled and executed according to the schedule.
 */
public interface Schedulable {

  Schedule getSchedule();

  /**
   * Execute the component.
   * <p>NOTE: this should be a lightweight method that executes quickly, to avoid blocking the
   * execution of other schedules. If it requires more work it should do this in a separate thread.</p>
   */
  void run();
}
