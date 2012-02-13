#!/bin/bash

rm -f .gitignore-new

for dir in `find . -path '*.svn*' -prune -o -type d -print | grep -v  -e "/build"`; do
  ruby -e 'printf("SVN dir: %-70s", ARGV[0])' "$dir" >&2
  svn info $dir > /dev/null 2>&1
  if [ "$?" -eq "0" ]; then
   svn propget "svn:ignore" $dir | ruby -e 'while $stdin.gets; (puts ARGV[0].gsub(/^\./, "") + "/" + $_) unless $_.strip.empty?; end' "$dir" > .temp
   if [ -s .temp ]; then
     echo " OK" >&2
     echo -e "\n\n# $dir" >> .gitignore-new
     cat .temp            >> .gitignore-new
   else
     echo " --" >&2
   fi
   rm .temp
  else
   echo " NOT svn controlled." >&2
  fi
done