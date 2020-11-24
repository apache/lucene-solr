package org.apache.lucene.search;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A Basic abstract query that all IndriQueries can extend to implement toString,
 * equals, getClauses, and iterator.
 *
 */
public abstract class IndriQuery extends Query implements Iterable<BooleanClause> {

	private List<BooleanClause> clauses;

	public IndriQuery(List<BooleanClause> clauses) {
		this.clauses = clauses;
	}

	@Override
	public abstract Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException;

	@Override
	public String toString(String field) {
		StringBuilder buffer = new StringBuilder();

		int i = 0;
		for (BooleanClause c : this) {
			buffer.append(c.getOccur().toString());

			Query subQuery = c.getQuery();
			if (subQuery instanceof BooleanQuery) { // wrap sub-bools in parens
				buffer.append("(");
				buffer.append(subQuery.toString(field));
				buffer.append(")");
			} else {
				buffer.append(subQuery.toString(field));
			}

			if (i != clauses.size() - 1) {
				buffer.append(" ");
			}
			i += 1;
		}

		return buffer.toString();
	}

	@Override
	public boolean equals(Object o) {
		return sameClassAs(o) && equalsTo(getClass().cast(o));
	}

	@Override
	public void visit(QueryVisitor visitor) {
		visitor.visitLeaf(this);
	}

	private boolean equalsTo(IndriQuery other) {
		return clauses.equals(other.clauses);
	}

	@Override
	public int hashCode() {
		int hashCode = Objects.hash(clauses);
		if (hashCode == 0) {
			hashCode = 1;
		}
		return hashCode;
	}

	@Override
	public Iterator<BooleanClause> iterator() {
		return clauses.iterator();
	}

	public List<BooleanClause> getClauses() {
		return this.clauses;
	}

}
