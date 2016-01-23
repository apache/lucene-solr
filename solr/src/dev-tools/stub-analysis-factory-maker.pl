#!/usr/bin/perl
my $ASL = q{
/**
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

};

# $Id:$
# $URL:$
#
# What this script does...
#
# 1) reads a list of fully qualified package.ClassNames from STDIN
# 2) gets a list of src dirs from command line
# 3) crawl the source dirs, looking for .java files corrisponding to 
#    the ClassNames, if a match is found, creates a factory for it in
#    the current working directory using info about the constructor
#    in the orriginal .java file
#
# Note...
#  * any ClassNames not found will be logged to stdout
#  * script assumes generated factorories in "org.apache.solr.analysis package
#  * factories will be compilable only if orriginal class had no special
#    constructor args.  otherwise it will have an abstract init method that
#    needs filled in.

use strict;
use warnings;
use File::Find;


my $errors = 0;

my %classes = ();
while (<STDIN>) {
    chomp;
    # skip anonymous classes
    if (!/\$/) {
	$classes{$_} = 1;
    }
}

find({wanted => \&wanted,
      no_chdir => 1,
      }, 
     @ARGV);

sub wanted {

    my $file = $File::Find::name;
    
    return unless $file =~ m{/([^/]*)\.java$};
    my $class = $1;
    
    open(my $f, "<", $file) or die "can't open $file: $!";
    my $data;
    {
	local $/; # slurp
	$data = <$f>;
    }
    close $f;

    # skip abstract classes
    return if ($data =~ m{abstract\s+(\w+\s+)*class\s+$class});
    
    my $pack = "EMPTYPACKAGE";
    if ($data =~ m/package\s+(.*);/) {
	$pack = $1;
    }

    my $fullname = "${pack}.${class}";
    # only looking for certain classes
    return unless $classes{$fullname};

    print STDERR "$file\n";

    my @imports = $data =~ m/import\s+.*;/g;
    
    if ($data =~ m{public \s+ ((?:\w+\s+)*) $class \s*\(\s* ([^\)]*) \) }sx) {
	my $modifiers = $1;
	my $argline = $2;
	
	my $mainArgType;
	my $mainArg;
	my @orderedArgs;
	
	my %args = map { my ($v,$k) = split /\s+/;
			 push @orderedArgs, $k;
			 if ($v =~ m/^(Reader|TokenStream)/) {
			     $mainArgType=$v;
			     $mainArg=$k;
			 }
			 ($k, $v)
			 } split /\s*,\s*/, $argline;
	
	# wacky, doesn't use Reader or TokenStream ... skip (maybe a Sink?)
	unless (defined $mainArgType) {
	    warn "$class doesn't have a constructor with a Reader or TokenStream\n";
	    return;
	}

	my $type = ("Reader" eq $mainArgType) ? "Tokenizer" : "TokenFilter";

	my $facClass = "${class}Factory";
	my $facFile = "${facClass}.java";

	if (-e $facFile) {
	    warn "$facFile already exists (maybe the return type isn't specific?)";
	    $errors++;
	    return;
	}
	open my $o, ">", $facFile
	    or die "can't write to $facFile: $!";

	print $o "$ASL\n";
	print $o "package org.apache.solr.analysis;\n";
	print $o "import ${pack}.*;\n";
	print $o "$_\n" foreach @imports;
	print $o "import java.util.Map;\n";
	print $o "public class ${facClass} extends Base${type}Factory {\n";
	foreach my $arg (@orderedArgs) {
	    print $o "  private $args{$arg} $arg;\n" unless $arg eq $mainArg;
	}
	if (1 < @orderedArgs) {
	    # we need to init something, stub it out
	    print $o "  public abstract void init(Map<String, String> args) {\n";
	    print $o "    super.init(args);\n";
	    print $o "    // ABSTRACT BECAUSE IT'S A STUB .. FILL IT IN\n";
	    print $o "  }\n";
	}
	print $o "  public $class create($mainArgType $mainArg) {\n";
	print $o "    return new $class(", join(",", @orderedArgs), ");\n";
	print $o "  }\n";
	print $o "}\n\n";
	close $o;
	
	delete $classes{$fullname}; # we're done with this one
    } else {
	print STDERR "can't stub $class (no public constructor?)\n";
	$errors++;
    }
}
    
if (keys %classes) {
    print STDERR "Can't stub (or find java files) for...\n";
    foreach (keys %classes) {
	print STDERR "$_\n";
    }
    $errors++;
}
exit -1 if $errors;

