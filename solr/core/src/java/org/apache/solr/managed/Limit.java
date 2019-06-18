package org.apache.solr.managed;

/**
 *
 */
public class Limit {
  public final float min, max, cost;

  public Limit() {
    this(Float.MIN_VALUE, Float.MAX_VALUE, 1.0f);
  }

  public Limit(float min, float max) {
    this(min, max, 1.0f);
  }

  public Limit(float min, float max, float cost) {
    if (cost <= 0.0f) {
      throw new IllegalArgumentException("cost must be > 0.0f");
    }
    this.min = min;
    this.max = max;
    this.cost = cost;
  }

  public float deltaOutsideLimit(float currentValue) {
    if (currentValue < min) {
      return currentValue - min;
    } else if (currentValue > max) {
      return currentValue - max;
    } else {
      return 0;
    }
  }
}
