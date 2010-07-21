package org.apache.lucene.index;

import java.util.TreeMap;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.ThreadSafeCloneableSortedMap;

public class BufferedDeletesInRAM {
  static class Delete {
    int flushCount;

    public Delete(int flushCount) {
      this.flushCount = flushCount;
    }
  }

  final static class DeleteTerm extends Delete {
    final Term term;

    public DeleteTerm(Term term, int flushCount) {
      super(flushCount);
      this.term = term;
    }
  }

  final static class DeleteTerms extends Delete {
    final Term[] terms;

    public DeleteTerms(Term[] terms, int flushCount) {
      super(flushCount);
      this.terms = terms;
    }
  }
  
  final static class DeleteQuery extends Delete {
    final Query query;

    public DeleteQuery(Query query, int flushCount) {
      super(flushCount);
      this.query = query;
    }
  }

  final ThreadSafeCloneableSortedMap<Long, Delete> deletes = ThreadSafeCloneableSortedMap
      .getThreadSafeSortedMap(new TreeMap<Long, Delete>());

  final void addDeleteTerm(Term term, long sequenceID, int numThreadStates) {
    deletes.put(sequenceID, new DeleteTerm(term, numThreadStates));
  }

  final void addDeleteTerms(Term[] terms, long sequenceID, int numThreadStates) {
    deletes.put(sequenceID, new DeleteTerms(terms, numThreadStates));
  }

  final void addDeleteQuery(Query query, long sequenceID, int numThreadStates) {
    deletes.put(sequenceID, new DeleteQuery(query, numThreadStates));
  }

  boolean hasDeletes() {
    return !deletes.isEmpty();
  }

  void clear() {
    deletes.clear();
  }

  int getNumDeletes() {
    return this.deletes.size();
  }
}
