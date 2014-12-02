#!/bin/bash

numServers=$1

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided, usage: clean.sh {numServers}"

cd ..

for (( i=1; i <= $numServers; i++ ))
do
  rm -r -f server$i
done

rm -r -f serverzk
rm -r -f server-lastlogs