#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# remove this script when problems are fixed
SRCDIR=$1
WWWSRCDIR=$2
TESTSRCDIR=$3
PROJECTDIR=$4
DESTDIR="${PROJECTDIR}/src/java/org/tartarus/snowball"
WWWDSTDIR="${PROJECTDIR}/src/resources/org/apache/lucene/analysis/snowball"
TESTDSTDIR="${PROJECTDIR}/src/test/org/apache/lucene/analysis/snowball"

trap 'echo "usage: ./snowball.sh <snowball> <snowball-website> <snowball-data> <analysis-common>" && exit 2' ERR
test $# -eq 4

trap 'echo "*** BUILD FAILED ***" $BASH_SOURCE:$LINENO: error: "$BASH_COMMAND" returned $?' ERR
set -eEuo pipefail

# reformats file indentation to kill the crazy space/tabs mix.
# prevents early blindness !
function reformat_java() {
  # convert tabs to 8 spaces, then reduce indent from 4 space to 2 space
  target=$1
  tmpfile=$(mktemp)
  cat ${target} | perl -p -e 's/\t/        /g' | perl -p -e 's/    /  /g' > ${tmpfile}
  mv ${tmpfile} ${target}
}

# generate stuff with existing makefile, just 'make' will try to do crazy stuff with e.g. python
# and likely fail. so only ask for our specific target.
(cd ${SRCDIR} && chmod a+x libstemmer/mkalgorithms.pl && make dist_libstemmer_java)

for file in "SnowballStemmer.java" "Among.java" "SnowballProgram.java"; do
  # add license header to files since they have none, otherwise rat will flip the fuck out
  echo "/*" > ${DESTDIR}/${file}
  cat ${SRCDIR}/COPYING >> ${DESTDIR}/${file}
  echo "*/" >> ${DESTDIR}/${file}
  cat ${SRCDIR}/java/org/tartarus/snowball/${file} >> ${DESTDIR}/${file}
  reformat_java ${DESTDIR}/${file}
done

rm ${DESTDIR}/ext/*Stemmer.java
rm -f ${TESTDSTDIR}/languages.txt
for file in ${SRCDIR}/java/org/tartarus/snowball/ext/*.java; do
  # title-case the classes (fooStemmer -> FooStemmer) so they obey normal java conventions
  base=$(basename $file)
  oldclazz="${base%.*}"
  # one-off
  if [ "${oldclazz}" == "kraaij_pohlmannStemmer" ]; then
    newclazz="KpStemmer"
  else
    newclazz=${oldclazz^}
  fi
  echo ${newclazz} | sed -e 's/Stemmer//' >> ${TESTDSTDIR}/languages.txt
  cat $file | sed "s/${oldclazz}/${newclazz}/g" > ${DESTDIR}/ext/${newclazz}.java
  reformat_java ${DESTDIR}/ext/${newclazz}.java
done

# regenerate test data
rm -f ${TESTDSTDIR}/test_languages.txt
rm -f ${TESTDSTDIR}/*.zip
for file in ${TESTSRCDIR}/*; do
  # look for input (voc.txt) and expected output (output.txt) without any special licenses (COPYING)
  if [ -f "${file}/voc.txt" ] && [ -f "${file}/output.txt" ] && [ ! -f "${file}/COPYING" ]; then
    language=$(basename ${file})
    if [ "${language}" == "kraaij_pohlmann" ]; then
      language="kp"
    fi
    # make the .zip reproducible if data hasn't changed.
    arbitrary_timestamp="200001010000"
    # some test files are yuge, randomly sample up to this amount
    row_limit="2000"
    tmpdir=$(mktemp -d)
    myrandom="openssl enc -aes-256-ctr -k ${arbitrary_timestamp} -nosalt -iv 0 -md md5"
    for data in "voc.txt" "output.txt"; do
      shuf -n ${row_limit} --random-source=<(${myrandom} < /dev/zero 2>/dev/null) ${file}/${data} > ${tmpdir}/${data} \
        && touch -t ${arbitrary_timestamp} ${tmpdir}/${data}
    done
    # explicitly set permissions in case someone has a crazy umask (otherwise zip will differ)
    chmod 644 ${tmpdir}/voc.txt ${tmpdir}/output.txt
    zip --quiet --junk-paths -X -9 ${TESTDSTDIR}/${language}.zip ${tmpdir}/voc.txt ${tmpdir}/output.txt
    echo "${language}" >> ${TESTDSTDIR}/test_languages.txt
    rm -r ${tmpdir}
  fi
done

# regenerate stopwords data
rm -f ${WWWDSTDIR}/*_stop.txt
for file in ${WWWSRCDIR}/algorithms/*/stop.txt; do
  language=$(basename $(dirname ${file}))
  cat > ${WWWDSTDIR}/${language}_stop.txt << EOF
 | From https://snowballstem.org/algorithms/${language}/stop.txt
 | This file is distributed under the BSD License.
 | See https://snowballstem.org/license.html
 | Also see https://opensource.org/licenses/bsd-license.html
 |  - Encoding was converted to UTF-8.
 |  - This notice was added.
 |
 | NOTE: To use this file with StopFilterFactory, you must specify format="snowball"
EOF
  case "$language" in
    danish)
      # clear up some slight mojibake on the website. TODO: fix this file!
      cat $file | sed 's/Ã¥/å/g' | sed 's/Ã¦/æ/g' >> ${WWWDSTDIR}/${language}_stop.txt
      ;;
    *)
      # try to confirm its really UTF-8
      iconv -f UTF-8 -t UTF-8 $file >> ${WWWDSTDIR}/${language}_stop.txt
      ;;
  esac
done
