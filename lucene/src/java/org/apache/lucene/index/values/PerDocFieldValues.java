package org.apache.lucene.index.values;

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
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;

/**
 * 
 * @lucene.experimental
 */
public interface PerDocFieldValues {

  public void setInt(long value);

  public void setFloat(float value);

  public void setFloat(double value);

  public void setBytes(BytesRef value, Type type);

  public void setBytes(BytesRef value, Type type, Comparator<BytesRef> comp);

  public BytesRef getBytes();

  public Comparator<BytesRef> bytesComparator();

  public double getFloat();

  public long getInt();

  public void setBytesComparator(Comparator<BytesRef> comp);

  public void setType(Type type);

  public Type type();

}