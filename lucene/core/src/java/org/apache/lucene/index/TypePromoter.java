package org.apache.lucene.index;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues.Type;

/**
 * Type promoter that promotes {@link DocValues} during merge based on
 * their {@link Type} and {@link #getValueSize()}
 * 
 * @lucene.internal
 */
class TypePromoter {

  private final static Map<Integer, Type> FLAGS_MAP = new HashMap<Integer, Type>();
  private static final TypePromoter IDENTITY_PROMOTER = new IdentityTypePromoter();
  public static final int VAR_TYPE_VALUE_SIZE = -1;

  private static final int IS_INT = 1 << 0;
  private static final int IS_BYTE = 1 << 1;
  private static final int IS_FLOAT = 1 << 2;
  /* VAR & FIXED == VAR */
  private static final int IS_VAR = 1 << 3;
  private static final int IS_FIXED = 1 << 3 | 1 << 4;
  /* if we have FIXED & FIXED with different size we promote to VAR */
  private static final int PROMOTE_TO_VAR_SIZE_MASK = ~(1 << 3);
  /* STRAIGHT & DEREF == STRAIGHT (dense values win) */
  private static final int IS_STRAIGHT = 1 << 5;
  private static final int IS_DEREF = 1 << 5 | 1 << 6;
  private static final int IS_SORTED = 1 << 7;
  /* more bits wins (int16 & int32 == int32) */
  private static final int IS_8_BIT = 1 << 8 | 1 << 9 | 1 << 10 | 1 << 11;
  private static final int IS_16_BIT = 1 << 9 | 1 << 10 | 1 << 11;
  private static final int IS_32_BIT = 1 << 10 | 1 << 11;
  private static final int IS_64_BIT = 1 << 11;

  private final Type type;
  private final int flags;
  private final int valueSize;

  /**
   * Returns a positive value size if this {@link TypePromoter} represents a
   * fixed variant, otherwise <code>-1</code>
   * 
   * @return a positive value size if this {@link TypePromoter} represents a
   *         fixed variant, otherwise <code>-1</code>
   */
  public int getValueSize() {
    return valueSize;
  }

  static {
    for (Type type : Type.values()) {
      TypePromoter create = create(type, VAR_TYPE_VALUE_SIZE);
      FLAGS_MAP.put(create.flags, type);
    }
  }

  /**
   * Creates a new {@link TypePromoter}
   * 
   * @param type
   *          the {@link Type} this promoter represents
   * @param flags
   *          the promoters flags
   * @param valueSize
   *          the value size if {@link #IS_FIXED} or <code>-1</code> otherwise.
   */
  protected TypePromoter(Type type, int flags, int valueSize) {
    this.type = type;
    this.flags = flags;
    this.valueSize = valueSize;
  }

  /**
   * Creates a new promoted {@link TypePromoter} based on this and the given
   * {@link TypePromoter} or <code>null</code> iff the {@link TypePromoter} 
   * aren't compatible.
   * 
   * @param promoter
   *          the incoming promoter
   * @return a new promoted {@link TypePromoter} based on this and the given
   *         {@link TypePromoter} or <code>null</code> iff the
   *         {@link TypePromoter} aren't compatible.
   */
  public TypePromoter promote(TypePromoter promoter) {

    int promotedFlags = promoter.flags & this.flags;
    TypePromoter promoted = create(FLAGS_MAP.get(promotedFlags), valueSize);
    if (promoted == null) {
      return promoted;
    }
    if ((promoted.flags & IS_BYTE) != 0 && (promoted.flags & IS_FIXED) == IS_FIXED) {
      if (this.valueSize == promoter.valueSize) {
        return promoted;
      }
      return create(FLAGS_MAP.get(promoted.flags & PROMOTE_TO_VAR_SIZE_MASK),
          VAR_TYPE_VALUE_SIZE);
    }
    return promoted;

  }

  /**
   * Returns the {@link Type} of this {@link TypePromoter}
   * 
   * @return the {@link Type} of this {@link TypePromoter}
   */
  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return "TypePromoter [type=" + type + ", sizeInBytes=" + valueSize + "]";
  }

  /**
   * Creates a new {@link TypePromoter} for the given type and size per value.
   * 
   * @param type
   *          the {@link Type} to create the promoter for
   * @param valueSize
   *          the size per value in bytes or <code>-1</code> iff the types have
   *          variable length.
   * @return a new {@link TypePromoter}
   */
  public static TypePromoter create(Type type, int valueSize) {
    if (type == null) {
      return null;
    }
    switch (type) {
    case BYTES_FIXED_DEREF:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_DEREF, valueSize);
    case BYTES_FIXED_SORTED:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_SORTED, valueSize);
    case BYTES_FIXED_STRAIGHT:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_STRAIGHT, valueSize);
    case BYTES_VAR_DEREF:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_DEREF, VAR_TYPE_VALUE_SIZE);
    case BYTES_VAR_SORTED:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_SORTED, VAR_TYPE_VALUE_SIZE);
    case BYTES_VAR_STRAIGHT:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_STRAIGHT, VAR_TYPE_VALUE_SIZE);
    case FIXED_INTS_16:
      return new TypePromoter(type,
          IS_INT | IS_FIXED | IS_STRAIGHT | IS_16_BIT, valueSize);
    case FIXED_INTS_32:
      return new TypePromoter(type,
          IS_INT | IS_FIXED | IS_STRAIGHT | IS_32_BIT, valueSize);
    case FIXED_INTS_64:
      return new TypePromoter(type,
          IS_INT | IS_FIXED | IS_STRAIGHT | IS_64_BIT, valueSize);
    case FIXED_INTS_8:
      return new TypePromoter(type, IS_INT | IS_FIXED | IS_STRAIGHT | IS_8_BIT,
          valueSize);
    case FLOAT_32:
      return new TypePromoter(type, IS_FLOAT | IS_FIXED | IS_STRAIGHT
          | IS_32_BIT, valueSize);
    case FLOAT_64:
      return new TypePromoter(type, IS_FLOAT | IS_FIXED | IS_STRAIGHT
          | IS_64_BIT, valueSize);
    case VAR_INTS:
      return new TypePromoter(type, IS_INT | IS_VAR | IS_STRAIGHT, VAR_TYPE_VALUE_SIZE);
    default:
      throw new IllegalStateException();
    }
  }

  /**
   * Returns a {@link TypePromoter} that always promotes to the type provided to
   * {@link #promote(TypePromoter)}
   */
  public static TypePromoter getIdentityPromoter() {
    return IDENTITY_PROMOTER;
  }

  private static class IdentityTypePromoter extends TypePromoter {

    public IdentityTypePromoter() {
      super(null, 0, -1);
    }

    @Override
    public TypePromoter promote(TypePromoter promoter) {
      return promoter;
    }
  }
}