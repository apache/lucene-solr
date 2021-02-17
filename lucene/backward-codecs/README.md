# Index backwards compatibility

This README describes the approach to maintaining compatibility with indices
from previous versions and gives guidelines for making format changes.

## Compatibility strategy

Lucene supports the ability to read segments created in older versions by
maintaining old codec classes along with their formats. When making a change
to a file format, we create a fresh format class and copy the existing one
into the backwards-codecs package.

These older formats are tested in two ways:
* Through unit tests like TestLucene80NormsFormat, which checks we can write
then read data using the old format
* Through TestBackwardsCompatibility, which loads indices created in previous
versions and checks that we can search them

## Making index format changes

As an example, let's say we're making a change to the norms file format, and
the current class in core is Lucene80NormsFormat. We'd perform the following
steps:

1. Create a new format with the target version for the changes, for example
Lucene90NormsFormat. This includes creating copies of its writer and reader
classes, as well as any helper classes. Make sure to copy unit tests too, like
TestLucene80NormsFormat.
2. Move the old Lucene80NormsFormat, along with its writer, reader, tests, and
helper classes to the backwards-codecs package. If the format will only be
used for reading, then delete the write-side logic and move it to a test-only
class like Lucene80RWNormsFormat to support unit tests. Note that most formats
only need read logic, but a small set including DocValuesFormat and
FieldInfosFormat will need to retain write logic since can be used to update
old segments.
3. Make a change to the new format!

## Internal format versions

Each format class maintains an internal version which is written into the
file header. Generally these internal versions should not be used to make
format changes. For any significant change, we prefer to use the
'copy-on-write' approach described above, even if it produces a fair amount of
duplicated code. This keeps the versioning strategy simple and clear, and
ensures that we unit test all older index formats.
