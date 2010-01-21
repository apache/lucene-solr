#!/bin/csh -f
set infile = $1
set outdir = $2

set name = $infile:h:t:uStemmer

exec $0:h/snowball $infile -o $outdir/$name -n $name -java
