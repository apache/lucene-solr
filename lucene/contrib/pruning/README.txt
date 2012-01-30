Static index pruning tools.
===========================

This package provides tools and API-s for static index pruning.

Static pruning is an approach that reduces size of the index
by removing terms and/or postings that are considered less
important, i.e. they don't affect the quality of top-N
retrieval too much.

There are several different strategies for pruning, each with
its own set of pros and cons. Plese consult the javadocs of
TermPruningPolicy subclasses that contain also references
to published papers on each method.

There is also a simple command-line driver class that
can apply some of the common pruning policies:

Usage: PruningTool -impl (tf | carmel | carmeltopk | ridf) (-in <path1> [-in <path2> ...]) -out <outPath> -t <NN> [-del f1,f2,..] [-conf <file>] [-topkk <NN>] [-topke <NN>] [-topkr <NN>]
  -impl (tf | carmel | carmeltopk | ridf) TermPruningPolicy implementation name: TF or CarmelUniform or or CarmelTopK or RIDFTerm
  -in path  path to the input index. Can specify multiple input indexes.
  -out path output path where the output index will be stored.
  -t NN default threshold value (minimum in-document frequency) for all terms
  -del f1,f2,.. comma-separated list of field specs to delete (postings, vectors & stored):
    field spec : fieldName ( ':' [pPsv] )
    where: p - postings, P - payloads, s - stored value, v - vectors
  -conf file  path to config file with per-term thresholds
  -topkk NN 'K' for Carmel TopK Pruning: number of guaranteed top scores
  -topke NN 'Epsilon' for Carmel TopK Pruning: largest meaningless score difference
  -topkr NN 'R' for Carmel TopK Pruning: planned maximal number of terms in a query on pruned index