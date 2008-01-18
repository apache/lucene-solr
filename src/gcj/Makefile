# Makefile for Lucene GCJ code
#
# Usually invoked by Ant.  Requires that core classes & jars are already built.

ROOT=../..
BUILD=$(ROOT)/build
DEST=$(BUILD)/gcj
CORE=$(BUILD)/classes/java
SRC=.

CORE_OBJ:=$(subst .jar,.a,$(wildcard $(BUILD)/lucene-core-[0-9]*.jar))
CORE_JAVA:=$(shell find $(ROOT)/src/java -name '*.java')

CORE_HEADERS=\
  $(CORE)/org/apache/lucene/store/IndexInput.h \
  $(CORE)/org/apache/lucene/util/BitVector.h \
  $(CORE)/org/apache/lucene/index/SegmentTermDocs.h

DEMO_JAR:=$(wildcard $(BUILD)/lucene-demo*.jar)
DEMO_OBJ:=$(subst .jar,.a,$(DEMO_JAR))

JAVA:=$(wildcard $(SRC)/org/apache/lucene/*/*.java)
JAVA_HEADERS:=$(subst $(SRC)/,$(DEST)/,$(subst .java,.h,$(JAVA)))
JAVA_OBJ:=$(DEST)/lucene-gcj.a

CNI:=$(wildcard $(SRC)/org/apache/lucene/*/*.cc)
CNI_OBJ:=$(subst $(SRC)/,$(DEST)/,$(subst .cc,.o,$(CNI)))

GCJ ?= gcj
GCJH ?= gcjh
GPLUSPLUS ?= g++

CFLAGS ?= -O3 -ffast-math 
GCJFLAGS ?= $(CFLAGS) -fno-bounds-check -fno-store-check

ifdef PROFILE_ARCS
  CFLAGS += -fprofile-arcs
endif

ifdef BRANCH_PROBABILITIES
  CFLAGS += -fbranch-probabilities
endif

LIBS = -lstdc++

# default rule builds command line executables
all: $(BUILD)/indexFiles $(BUILD)/searchFiles

# Compile Lucene Core code specially
# GCJ can do more inlining when it compiles .java files than .class
# files, but not all of Lucene's core .java files are yet compilable
# by GCJ. (In particular, GCJ has problems with anonymous ctors that
# throw exceptions used in the Span and Sort code.)  So we compile
# those that GCJ accepts from .java files, and those that GCJ does not
# yet accept from .class files.
# NOTE: Change when http://gcc.gnu.org/bugzilla/show_bug.cgi?id=15501 is fixed.
$(CORE_OBJ) : $(CORE_JAVA)
	$(GCJ) $(GCJFLAGS) -c -I $(CORE) -o $@ `find $(ROOT)/src/java -name '*.java' -not -name '*Sort*' -not -name 'Span*'` `find $(CORE) -name '*.class' -name '*Sort*' -or -name 'Span*'`

# generate object code from jar files using gcj
%.a : %.jar
	$(GCJ) $(GCJFLAGS) -c -I $(CORE) -o $@ $<

# don't delete generated headers -- they're handy for debugging
.PRECIOUS : $(CORE)/%.h $(DEST)/%.h

# generate headers from .class files using gcjh
$(CORE)/%.h : $(CORE)/%.class
	$(GCJH) --classpath=$(CORE) -d $(CORE) \
	 $(subst /,.,$(subst .class,,$(subst $(CORE)/,,$<)))

# generate headers from .class files using gcjh
$(DEST)/%.h : $(DEST)/%.class
	$(GCJH) --classpath=$(DEST) -d $(DEST) \
	 $(subst /,.,$(subst .class,,$(subst $(DEST)/,,$<)))

# generate object code for c++ code using g++
$(DEST)/%.o : $(SRC)/%.cc $(DEST)/%.h $(CORE_HEADERS) $(JAVA_HEADERS)
	$(GPLUSPLUS) $(CFLAGS) -c -I $(CORE) -I $(DEST) -o $@ $<

# generate class from java using gcj
$(DEST)/%.class : $(SRC)/%.java
	mkdir -p $(dir $@)
	$(GCJ) -C -I $(CORE) -d $(DEST) $<

# generate object code for .java files using gcj
$(JAVA_OBJ) : $(JAVA)
	mkdir -p $(dir $@)
	$(GCJ) $(GCJFLAGS) -c -I $(CORE) -I $(DEST) -o $@ $^

# generate c++ stubs for a class using gcjh
$(DEST)/%.cc : $(DEST)/%.class
	$(GCJH) -stubs --classpath=$(DEST) -d $(DEST) \
	 $(subst /,.,$(subst .class,,$(subst $(DEST)/,,$<)))

# generate assembly code from c++ code for perusal
$(DEST)/%.s : $(SRC)/%.cc $(DEST)/%.h $(CORE_HEADERS) $(JAVA_HEADERS)
	$(GPLUSPLUS) $(CFLAGS) -S -I $(CORE) -I $(DEST) -o $@ $<

# list of all object code to be linked
OBJ = $(CORE_OBJ) $(DEMO_OBJ) $(JAVA_OBJ) $(CNI_OBJ)

USE_GCJ_DIRECTORY =\
 -Dorg.apache.lucene.FSDirectory.class=org.apache.lucene.store.GCJDirectory

USE_GCJ_SEGMENT_READER =\
 -Dorg.apache.lucene.SegmentReader.class=org.apache.lucene.index.GCJSegmentReader

PROPS = $(USE_GCJ_DIRECTORY) $(USE_GCJ_SEGMENT_READER)

# link together various applications
$(BUILD)/indexFiles: $(OBJ)
	$(GCJ) $(GCJFLAGS) $(PROPS) $(LIBS) $^ -o $@ \
         --main=org.apache.lucene.demo.IndexFiles

$(BUILD)/searchFiles: $(OBJ)
	$(GCJ) $(GCJFLAGS) $(PROPS) $(LIBS) $^ -o $@ \
         --main=org.apache.lucene.demo.SearchFiles

# remove generated files
clean:
	rm -rf $(DEST) $(BUILD)/{*.a,indexFiles,searchFiles}

