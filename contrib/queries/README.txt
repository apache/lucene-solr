This module contains a number of filter and query objects that add to core lucene.

==== TrieRangeFilter/TrieRangeQuery - This package provides fast numeric range queries/filters on long,
double or Date fields based on trie structures. Numerical values are stored in a special string-encoded
format with variable precision (all numerical values like doubles, longs, and timestamps are converted
to lexicographic sortable string representations).  A range is then divided recursively into multiple
intervals for searching: The center of the range is searched only with the lowest possible precision
in the trie, the boundaries are matched more exactly. This reduces the number of terms and thus improves
dramatically the performance of range queries, on the cost of larger index sizes.

==== The "MoreLikeThis" class from the "similarity" module has been copied into here.
If people are generally happy with this move then the similarity module can be deleted, or at least a 
"Moved to queries module..." note left in its place.

==== FuzzyLikeThis - mixes the behaviour of FuzzyQuery and MoreLikeThis but with special consideration
of fuzzy scoring factors. This generally produces good results for queries where users may provide details in a number of 
fields and have no knowledge of boolean query syntax and also want a degree of fuzzy matching. The query is fast because, like
MoreLikeThis, it optimizes the query to only the most distinguishing terms.

==== BoostingQuery - effectively demotes search results that match a given query. 
Unlike the "NOT" clause, this still selects documents that contain undesirable terms, 
but reduces the overall score of docs containing these terms.

==== TermsFilter -  Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in 
a sequence. An example might be a collection of primary keys from a database query result or perhaps 
a choice of "category" labels picked by the end user.


Mark Harwood & Uwe Schindler
05/12/2008
