package org.apache.solr.store.blob.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DeduplicatingList}.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class DeduplicationListTest extends Assert {

    private DeduplicatingList<String, DeduplicatableCar> dedupeList;

    @Before
    public void createList() {
        dedupeList = new DeduplicatingList<>(100, new OldCarMerger());
    }

    /**
     * Tests puts into the list that do not merge.
     */
    @Test
    public void nonmergePutGet() throws Exception {
        // Put two different cars in the list, verify we get them in the right order.
        DeduplicatableCar leon = new DeduplicatableCar("Leon", 2017);
        DeduplicatableCar renault = new DeduplicatableCar("Dauphine", 1958);
        dedupeList.addDeduplicated(leon, false);
        dedupeList.addDeduplicated(renault, false);

        assertEquals("list size after two non merged adds", 2, dedupeList.size());

        assertEquals("expected first put to be returned first", leon, dedupeList.removeFirst());
        assertEquals("expected second put to be returned second", renault, dedupeList.removeFirst());

        assertEquals("list should be empty", 0, dedupeList.size());
    }

    /**
     * Tests puts into the list that merge.
     */
    @Test
    public void mergePutGet() throws Exception {
        // Verify merges occur
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 2017), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 1999), false);

        assertEquals("two cars expected merged but were not", 1, dedupeList.size());

        // Because we had a merge in the list, the merge result might not be equal to any of the merged objects
        // (well in this case it will given we only have the model and the year, but in general it will not, and
        // since we didn't implement equals() in DeduplicatableCar comparing to the inserted object wouldn't work anyway)
        assertEquals("expected the merged car to have the oldest of the two years", 1999, dedupeList.removeFirst().year);
    }

    /**
     * Tests 4 interleaved puts into the list that merge 2 by 2.
     */
    @Test
    public void interleavedMerges1() throws Exception {
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 2018), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1963), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 1999), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1958), false);

        // We expect Leon to be returned first (since it was inserted first) and be returned with 1999 since it's the older date
        // Then the Dauphine car should be returned second, with the older 1958 date as well

        assertEquals("list size expected 2 as entries merged", 2, dedupeList.size());
        DeduplicatableCar car1 = dedupeList.removeFirst();
        assertEquals("Wrong car model", "Leon", car1.model);
        assertEquals("Wrong year", 1999, car1.year);

        DeduplicatableCar car2 = dedupeList.removeFirst();
        assertEquals("Wrong car model", "Dauphine", car2.model);
        assertEquals("Wrong year", 1958, car2.year);
    }

    /**
     * Tests 4 interleaved puts into the list that merge 2 by 2, inserted in a different order from previous test.
     */
    @Test
    public void interleavedMerges2() throws Exception {
        // Note Dauphine 1958 inserted first and Dauphine 1963 second (difference with test interleavedMerges1)
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 2018), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1958), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 1999), false);
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1963), false);

        // We expect Leon to be returned first (since it was inserted first) and be returned with 1999 since it's the older date
        // Then the Dauphine car should be returned second, with the older 1958 date as well.
        // The fact that we've changed the order of adding the two Daupine entries shoud not matter.

        assertEquals("list size expected 2 as entries merged", 2, dedupeList.size());
        DeduplicatableCar car1 = dedupeList.removeFirst();
        assertEquals("Wrong car model", "Leon", car1.model);
        assertEquals("Wrong year", 1999, car1.year);

        DeduplicatableCar car2 = dedupeList.removeFirst();
        assertEquals("Wrong car model", "Dauphine", car2.model);
        assertEquals("Wrong year", 1958, car2.year);
    }

    /**
     * Test list full
     */
    @Test
    public void listFull() throws Exception {
        // Replacing the standard list by one with capacity of 1
        dedupeList = new DeduplicatingList<>(1, new OldCarMerger());

        dedupeList.addDeduplicated(new DeduplicatableCar("Leon", 2018), false);

        // Now you have to trust me that if the line below is executed, the test hangs (I did test it)
        // dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1963), false);

        // But we can insert above max size if we set isReenqueue to true
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1963), true);

        // We are allowed to merge into this additional entry even though we don't set isReenqueue to true
        dedupeList.addDeduplicated(new DeduplicatableCar("Dauphine", 1958), false);

        // And of course we should be getting the Leon first, the Dauphine second
        assertEquals("Wrong car model", "Leon", dedupeList.removeFirst().model);
        assertEquals("Wrong car model", "Dauphine", dedupeList.removeFirst().model);

        // And given there's nothing left in the list, we can do a new add
        dedupeList.addDeduplicated(new DeduplicatableCar("Sergio", 2014), false);
    }


    /**
     * A deduplicatable representation of a car. Deduplicating on car model.
     */
    static class DeduplicatableCar implements DeduplicatingList.Deduplicatable<String> {
        final String model;
        final int year;

        DeduplicatableCar(String model, int year) {
            this.model = model;
            this.year = year;
        }

        @Override
        public String getDedupeKey() {
            return this.model;
        }
    }

    /**
     * Given the {@list DeduplicatableCar}, this merger method keeps the oldest of the two cars. For example we're collecting
     * old cars and the older the better :)
     */
    static class OldCarMerger implements DeduplicatingList.Merger<String, DeduplicatableCar> {
        @Override
        public DeduplicatableCar merge(DeduplicatableCar c1, DeduplicatableCar c2) {
            if (!c1.model.equals(c2.model)) {
                return null;
            } else {
                return new DeduplicatableCar(c1.model, Math.min(c1.year, c2.year));
            }
        }
    }
}
