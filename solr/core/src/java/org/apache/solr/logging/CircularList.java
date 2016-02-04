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
package org.apache.solr.logging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * FIFO Circular List.
 * 
 * Once the size is reached, it will overwrite previous entries
 * 
 */
public class CircularList<T> implements Iterable<T>
{
  private T[] data;
  private int head=0;
  private int tail=0;
  private int size=0;

  @SuppressWarnings("unchecked")
  public CircularList(int size) {
    data = (T[])new Object[size];
  }

  @SuppressWarnings("unchecked")
  public synchronized void resize(int newsize) {
    if(newsize==this.size) return;
    
    T[] vals = (T[])new Object[newsize];
    int i = 0;
    if(newsize>size) {
      for(i=0; i<size; i++) {
        vals[i] = data[convert(i)];
      }
    }
    else {
      int off=size-newsize;
      for(i=0; i<newsize; i++) {
        vals[i] = data[convert(i+off)];
      }
    }
    data = vals;
    head = 0;
    tail = i;
  }

  private int convert(int index) {
    return (index + head) % data.length;
  }

  public boolean isEmpty() {
    return head == tail; // or size == 0
  }

  public int size() {
    return size;
  }
  
  public int getBufferSize() {
    return data.length;
  }

  private void checkIndex(int index) {
    if (index >= size || index < 0)
      throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
  }

  public T get(int index) {
    checkIndex(index);
    return data[convert(index)];
  }

  public synchronized void add(T o) {
    data[tail] = o;
    tail = (tail+1)%data.length;
    if( size == data.length ) {
      head = (head+1)%data.length;
    }
    size++;
    if( size > data.length ) {
      size = data.length;
    }
  }

  public synchronized void clear() {
    for( int i=0; i<data.length; i++ ) {
      data[i] = null;  // for GC
    }
    head = tail = size = 0;
  }

  public List<T> toList()
  {
    ArrayList<T> list = new ArrayList<>( size );
    for( int i=0; i<size; i++ ) {
      list.add( data[convert(i)] );
    }
    return list;
  }

  @Override
  public String toString()
  {
    StringBuilder str = new StringBuilder();
    str.append( "[" );
    for( int i=0; i<size; i++ ) {
      if( i > 0 ) {
        str.append( "," );
      }
      str.append( data[convert(i)] );
    }
    str.append( "]" );
    return str.toString();
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx<size;
      }

      @Override
      public T next() {
        return get( idx++ );
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
