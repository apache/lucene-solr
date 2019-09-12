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
package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardIndexReaderFactory;

public class TrollingIndexReaderFactory extends StandardIndexReaderFactory {

  private static volatile Trap trap;
  private final static BlockingQueue<List<Object>> lastStacktraces = new LinkedBlockingQueue<List<Object>>();
  private final static long startTime = ManagementFactory.getRuntimeMXBean().getStartTime();
  private static final int keepStackTraceLines = 20;
  protected static final int maxTraces = 4;

  
  private static Trap setTrap(Trap troll) {
    trap = troll;  
    return troll;
  }
  
  public static abstract class Trap implements Closeable{
    protected abstract boolean shouldExit();
    public abstract boolean hasCaught();
    @Override
    public final void close() throws IOException {
      setTrap(null);
    }
    @Override
    public abstract String toString();
    
    public static void dumpLastStackTraces(org.slf4j.Logger log) {
      ArrayList<List<Object>> stacks = new ArrayList<>();
      lastStacktraces.drainTo(stacks);
      StringBuilder out = new StringBuilder("the last caught stacktraces: \n");
      for(List<Object> stack : stacks) {
        int l=0;
        for (Object line : stack) {
          if (l++>0) {
            out.append('\t');
          }
          out.append(line);
          out.append('\n');
        }
        out.append('\n');
      }
      log.error("the last caught traces {}", out);
    }
  }

  static final class CheckMethodName implements Predicate<StackTraceElement> {
    private final String methodName;
  
    CheckMethodName(String methodName) {
      this.methodName = methodName;
    }
  
    @Override
    public boolean test(StackTraceElement trace) {
      return trace.getMethodName().equals(methodName);
    }
    
    @Override
    public String toString() {
      return "hunting for "+methodName+"()";
    }
  }

  public static Trap catchClass(String className) {
    return catchClass(className, ()->{});
  }
  
  public static Trap catchClass(String className, Runnable onCaught) {
    Predicate<StackTraceElement> judge = new Predicate<StackTraceElement>() {
      @Override
      public boolean test(StackTraceElement trace) {
        return trace.getClassName().indexOf(className)>=0;
      }
      @Override
      public String toString() {
        return "className contains "+className;
      }
    };
    return catchTrace(judge, onCaught) ;        
  }
  
  public static Trap catchTrace(Predicate<StackTraceElement> judge, Runnable onCaught) {
    return setTrap(new Trap() {
      
      private boolean trigered;

      @Override
      protected boolean shouldExit() {
        Exception e = new Exception("stack sniffer"); 
        e.fillInStackTrace();
        StackTraceElement[] stackTrace = e.getStackTrace();
        for(StackTraceElement trace : stackTrace) {
          if (judge.test(trace)) {
            trigered = true; 
            recordStackTrace(stackTrace);
            onCaught.run();
            return true;
          }
        }
        return false;
      }

      @Override
      public boolean hasCaught() {
        return trigered;
      }

      @Override
      public String toString() {
        return ""+judge;
      }
    });
  }
  
  public static Trap catchCount(int boundary) {
    return setTrap(new Trap() {
      
      private AtomicInteger count = new AtomicInteger();
    
      @Override
      public String toString() {
        return ""+count.get()+"th tick of "+boundary+" allowed";
      }
      
      private boolean trigered;

      @Override
      protected boolean shouldExit() {
        int now = count.incrementAndGet();
        boolean trigger = now==boundary 
            || (now>boundary && LuceneTestCase.rarely(LuceneTestCase.random()));
        if (trigger) {
          Exception e = new Exception("stack sniffer"); 
          e.fillInStackTrace();
          recordStackTrace(e.getStackTrace());
          trigered = true;
        } 
        return trigger;
      }

      @Override
      public boolean hasCaught() {
        return trigered;
      }
    });
  }
  
  private static void recordStackTrace(StackTraceElement[] stackTrace) {
    //keep the last n limited traces. 
    //e.printStackTrace();
    ArrayList<Object> stack = new ArrayList<Object>();
    stack.add(""+ (new Date().getTime()-startTime)+" ("+Thread.currentThread().getName()+")");
    for (int l=2; l<stackTrace.length && l<keepStackTraceLines; l++) {
      stack.add(stackTrace[l]);
    }
    lastStacktraces.add(stack);
    // triming queue 
    while(lastStacktraces.size()>maxTraces) {
      try {
        lastStacktraces.poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
    }
  }

  @Override
  public DirectoryReader newReader(Directory indexDir, SolrCore core) throws IOException {
    DirectoryReader newReader = super.newReader(indexDir, core);
    return wrap(newReader);
  }

  private ExitableDirectoryReader wrap(DirectoryReader newReader) throws IOException {
    return new ExitableDirectoryReader(newReader, new QueryTimeout() {
      @Override
      public boolean shouldExit() {
        return trap!=null && trap.shouldExit();
      }
      
      @Override
      public String toString() {
        return ""+trap;
      }
    });
  }

  @Override
  public DirectoryReader newReader(IndexWriter writer, SolrCore core) throws IOException {
    DirectoryReader newReader = super.newReader(writer, core);
    return wrap(newReader);
  }
}
