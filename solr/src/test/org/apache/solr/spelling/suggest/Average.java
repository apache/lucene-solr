package org.apache.solr.spelling.suggest;

import java.util.List;
import java.util.Locale;

/**
 * Average with standard deviation.
 */
final class Average
{
    /**
     * Average (in milliseconds).
     */
    public final double avg;

    /**
     * Standard deviation (in milliseconds).
     */
    public final double stddev;

    /**
     * 
     */
    Average(double avg, double stddev)
    {
        this.avg = avg;
        this.stddev = stddev;
    }

    public String toString()
    {
        return String.format(Locale.ENGLISH, "%.0f [+- %.2f]", 
            avg, stddev);
    }

    static Average from(List<Double> values)
    {
        double sum = 0;
        double sumSquares = 0;

        for (double l : values)
        {
            sum += l;
            sumSquares += l * l;
        }

        double avg = sum / (double) values.size();
        return new Average(
            (sum / (double) values.size()), 
            Math.sqrt(sumSquares / (double) values.size() - avg * avg));
    }
}