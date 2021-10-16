# Developer Scripts

This folder contains various useful scripts for developers, mostly related to
releasing new versions of Lucene/Solr and testing those.

Python scripts require Python 3. To install necessary python modules, please run:

    pip3 install -r requirements.txt

## Scripts description

### smokeTestRelease.py

Used to validate new release candidates (RC). The script downloads an RC from a URL
or local folder, then runs a number of sanity checks on the artifacts, and then runs
the full tests.

    usage: smokeTestRelease.py [-h] [--tmp-dir PATH] [--not-signed]
                               [--local-keys PATH] [--revision REVISION]
                               [--version X.Y.Z(-ALPHA|-BETA)?]
                               [--test-java12 JAVA12_HOME] [--download-only]
                               url ...
    
    Utility to test a release.
    
    positional arguments:
      url                   Url pointing to release to test
      test_args             Arguments to pass to ant for testing, e.g.
                            -Dwhat=ever.
    
    optional arguments:
      -h, --help            show this help message and exit
      --tmp-dir PATH        Temporary directory to test inside, defaults to
                            /tmp/smoke_lucene_$version_$revision
      --not-signed          Indicates the release is not signed
      --local-keys PATH     Uses local KEYS file instead of fetching from
                            https://archive.apache.org/dist/lucene/KEYS
      --revision REVISION   GIT revision number that release was built with,
                            defaults to that in URL
      --version X.Y.Z(-ALPHA|-BETA)?
                            Version of the release, defaults to that in URL
      --test-java12 JAVA12_HOME
                            Path to Java12 home directory, to run tests with if
                            specified
      --download-only       Only perform download and sha hash check steps
    
    Example usage:
    python3 -u dev-tools/scripts/smokeTestRelease.py https://dist.apache.org/repos/dist/dev/lucene/lucene-solr-6.0.1-RC2-revc7510a0...

### releaseWizard.py

The Release Wizard guides the Release Manager through the release process step 
by step, helping you to to run the right commands in the right order, generating
e-mail templates with the correct texts, versions, paths etc, obeying
the voting rules and much more. It also serves as a documentation of all the
steps, with timestamps, preserving log files from each command etc, showing only
the steps and commands required for a major/minor/bugfix release. It also lets
you generate a full Asciidoc guide for the release. The wizard will execute many 
of the other tools in this folder. 

    usage: releaseWizard.py [-h] [--dry-run] [--init]
    
    Script to guide a RM through the whole release process
    
    optional arguments:
      -h, --help  show this help message and exit
      --dry-run   Do not execute any commands, but echo them instead. Display
                  extra debug info
      --init      Re-initialize root and version
    
    Go push that release!

### buildAndPushRelease.py

    usage: buildAndPushRelease.py [-h] [--no-prepare] [--local-keys PATH]
                                  [--push-local PATH] [--sign KEYID]
                                  [--rc-num NUM] [--root PATH] [--logfile PATH]
    
    Utility to build, push, and test a release.
    
    optional arguments:
      -h, --help         show this help message and exit
      --no-prepare       Use the already built release in the provided checkout
      --local-keys PATH  Uses local KEYS file to validate presence of RM's gpg key
      --push-local PATH  Push the release to the local path
      --sign KEYID       Sign the release with the given gpg key
      --rc-num NUM       Release Candidate number. Default: 1
      --root PATH        Root of Git working tree for lucene-solr. Default: "."
                         (the current directory)
      --logfile PATH     Specify log file path (default /tmp/release.log)
    
    Example usage for a Release Manager:
    python3 -u dev-tools/scripts/buildAndPushRelease.py --push-local /tmp/releases/6.0.1 --sign 6E68DA61 --rc-num 1

### addBackcompatIndexes.py

    usage: addBackcompatIndexes.py [-h] [--force] [--no-cleanup] [--temp-dir DIR]
                                   version
    
    Add backcompat index and test for new version.  See:
    http://wiki.apache.org/lucene-java/ReleaseTodo#Generate_Backcompat_Indexes
    
    positional arguments:
      version         Version to add, of the form X.Y.Z
    
    optional arguments:
      -h, --help      show this help message and exit
      --force         Redownload the version and rebuild, even if it already
                      exists
      --no-cleanup    Do not cleanup the built indexes, so that they can be reused
                      for adding to another branch
      --temp-dir DIR  Temp directory to build backcompat indexes within

### addVersion.py

    usage: addVersion.py [-h] version
    
    Add a new version to CHANGES, to Version.java, lucene/version.properties and
    solrconfig.xml files
    
    positional arguments:
      version
    
    optional arguments:
      -h, --help  show this help message and exit

### releasedJirasRegex.py

Pulls out all JIRAs mentioned at the beginning of bullet items
under the given version in the given CHANGES.txt file
and prints a regular expression that will match all of them

    usage: releasedJirasRegex.py [-h] version changes
    
    Prints a regex matching JIRAs fixed in the given version by parsing the given
    CHANGES.txt file
    
    positional arguments:
      version     Version of the form X.Y.Z
      changes     CHANGES.txt file to parse
    
    optional arguments:
      -h, --help  show this help message and exit

### reproduceJenkinsFailures.py

    usage: reproduceJenkinsFailures.py [-h] [--no-git] [--iters N] URL
    
    Must be run from a Lucene/Solr git workspace. Downloads the Jenkins
    log pointed to by the given URL, parses it for Git revision and failed
    Lucene/Solr tests, checks out the Git revision in the local workspace,
    groups the failed tests by module, then runs
    'ant test -Dtest.dups=%d -Dtests.class="*.test1[|*.test2[...]]" ...'
    in each module of interest, failing at the end if any of the runs fails.
    To control the maximum number of concurrent JVMs used for each module's
    test run, set 'tests.jvms', e.g. in ~/lucene.build.properties
    
    positional arguments:
      URL         Points to the Jenkins log to parse
    
    optional arguments:
      -h, --help  show this help message and exit
      --no-git    Do not run "git" at all
      --iters N   Number of iterations per test suite (default: 5)

### githubPRs.py

    usage: githubPRs.py [-h] [--json] [--token TOKEN]
    
    Find open Pull Requests that need attention
    
    optional arguments:
      -h, --help     show this help message and exit
      --json         Output as json
      --token TOKEN  Github access token in case you query too often anonymously

### gitignore-gen.sh

TBD

