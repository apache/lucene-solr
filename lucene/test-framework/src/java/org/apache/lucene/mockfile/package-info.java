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
 * Support for testing/debugging with virtual filesystems 
 * <p>
 * The primary classes are:
 * <ul>
 *   <li>{@link org.apache.lucene.mockfile.LeakFS}: Fails tests if they leave open filehandles.
 *   <li>{@link org.apache.lucene.mockfile.VerboseFS}: Prints destructive filesystem operations to infostream.
 *   <li>{@link org.apache.lucene.mockfile.WindowsFS}: Acts like windows.
 *   <li>{@link org.apache.lucene.mockfile.DisableFsyncFS}: Makes actual fsync calls a no-op.
 *   <li>{@link org.apache.lucene.mockfile.ExtrasFS}: Adds 'bonus' files to directories.
 *   <li>{@link org.apache.lucene.mockfile.ShuffleFS}: Directory listings in an unpredictable but deterministic order.
 * </ul>
 */
package org.apache.lucene.mockfile;
