package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * A {@link Query} for drill-down over {@link CategoryPath categories}. You
 * should call {@link #add(CategoryPath...)} for every group of categories you
 * want to drill-down over. Each category in the group is {@code OR'ed} with
 * the others, and groups are {@code AND'ed}.
 * <p>
 * <b>NOTE:</b> if you choose to create your own {@link Query} by calling
 * {@link #term}, it is recommended to wrap it with {@link ConstantScoreQuery}
 * and set the {@link ConstantScoreQuery#setBoost(float) boost} to {@code 0.0f},
 * so that it does not affect the scores of the documents.
 * 
 * @lucene.experimental
 */
public final class DrillDownQuery extends Query {

  /** Return a drill-down {@link Term} for a category. */
  public static final Term term(FacetIndexingParams iParams, CategoryPath path) {
    CategoryListParams clp = iParams.getCategoryListParams(path);
    char[] buffer = new char[path.fullPathLength()];
    iParams.drillDownTermText(path, buffer);
    return new Term(clp.field, String.valueOf(buffer));
  }
  
  private final BooleanQuery query;
  private final Set<String> drillDownDims = new HashSet<String>();

  private final FacetIndexingParams fip;

  /* Used by clone() */
  private DrillDownQuery(FacetIndexingParams fip, BooleanQuery query, Set<String> drillDownDims) {
    this.fip = fip;
    this.query = query.clone();
    this.drillDownDims.addAll(drillDownDims);
  }

  /**
   * Creates a new {@link DrillDownQuery} without a base query, which means that
   * you intend to perfor a pure browsing query (equivalent to using
   * {@link MatchAllDocsQuery} as base.
   */
  public DrillDownQuery(FacetIndexingParams fip) {
    this(fip, null);
  }
  
  /**
   * Creates a new {@link DrillDownQuery} over the given base query. Can be
   * {@code null}, in which case the result {@link Query} from
   * {@link #rewrite(IndexReader)} will be a pure browsing query, filtering on
   * the added categories only.
   */
  public DrillDownQuery(FacetIndexingParams fip, Query baseQuery) {
    query = new BooleanQuery(true); // disable coord
    if (baseQuery != null) {
      query.add(baseQuery, Occur.MUST);
    }
    this.fip = fip;
  }

  /**
   * Adds one dimension of drill downs; if you pass multiple values they are
   * OR'd, and then the entire dimension is AND'd against the base query.
   */
  public void add(CategoryPath... paths) {
    Query q;
    String dim = paths[0].components[0];
    if (drillDownDims.contains(dim)) {
      throw new IllegalArgumentException("dimension '" + dim + "' was already added");
    }
    if (paths.length == 1) {
      if (paths[0].length == 0) {
        throw new IllegalArgumentException("all CategoryPaths must have length > 0");
      }
      q = new TermQuery(term(fip, paths[0]));
    } else {
      BooleanQuery bq = new BooleanQuery(true); // disable coord
      for (CategoryPath cp : paths) {
        if (cp.length == 0) {
          throw new IllegalArgumentException("all CategoryPaths must have length > 0");
        }
        if (!cp.components[0].equals(dim)) {
          throw new IllegalArgumentException("multiple (OR'd) drill-down paths must be under same dimension; got '" 
              + dim + "' and '" + cp.components[0] + "'");
        }
        bq.add(new TermQuery(term(fip, cp)), Occur.SHOULD);
      }
      q = bq;
    }
    drillDownDims.add(dim);

    final ConstantScoreQuery drillDownQuery = new ConstantScoreQuery(q);
    drillDownQuery.setBoost(0.0f);
    query.add(drillDownQuery, Occur.MUST);
  }

  @Override
  public DrillDownQuery clone() {
    return new DrillDownQuery(fip, query, drillDownDims);
  }
  
  @Override
  public int hashCode() {
    return query.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DrillDownQuery)) {
      return false;
    }
    
    DrillDownQuery other = (DrillDownQuery) obj;
    return query.equals(other.query);
  }
  
  @Override
  public Query rewrite(IndexReader r) throws IOException {
    if (query.clauses().size() == 0) {
      // baseQuery given to the ctor was null + no drill-downs were added
      // note that if only baseQuery was given to the ctor, but no drill-down terms
      // is fine, since the rewritten query will be the original base query.
      throw new IllegalStateException("no base query or drill-down categories given");
    }
    return query;
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }
  
}
