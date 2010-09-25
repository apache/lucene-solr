package org.apache.lucene.search.cache;

public class SimpleEntryKey extends EntryKey
{
  public final Class clazz;
  public final Object[] args;
  public final int hash;

  public SimpleEntryKey( Class clazz, Object ... args ) {
    this.clazz = clazz;
    this.args = args;

    int hash = clazz.hashCode();
    if( args != null ) {
      for( Object obj : args ) {
        hash ^= obj.hashCode();
      }
    }
    this.hash = hash;
  }

  @Override
  public boolean equals(Object obj) {
    if( obj instanceof SimpleEntryKey ) {
      SimpleEntryKey key = (SimpleEntryKey)obj;
      if( key.hash != hash ||
          key.clazz != clazz ||
          key.args.length != args.length ) {
        return false;
      }

      // In the off chance that the hash etc is all the same
      // we should actually check the values
      for( int i=0; i<args.length; i++ ) {
        if( !args[i].equals( key.args[i] ) ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append( '[' ).append( clazz.getName() ).append( ':' );
    for( Object v : args ) {
      str.append( v ).append( ':' );
    }
    str.append( hash ).append( ']' );
    return str.toString();
  }
}
