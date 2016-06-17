package com.orientechnologies.orient.core.index.lsmtree;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

@Test
public class OCuckooFilterTest {
  public void addSingleItem() {
    OCuckooFilter filter = new OCuckooFilter(10);
    final byte[] key = new byte[] { 10, 12 };

    Assert.assertTrue(filter.add(key));
    Assert.assertTrue(filter.contains(key));
    Assert.assertTrue(!filter.contains(new byte[] { 9, 2 }));

    filter.remove(key);
    Assert.assertTrue(!filter.contains(key));
    Assert.assertTrue(!filter.contains(new byte[] { 9, 2 }));
  }

  public void addItemsTillItPossible() {
    int capacity = 1 << 10;
    OCuckooFilter filter = new OCuckooFilter(capacity);

    final long seed = System.currentTimeMillis();
    System.out.println("addItemsTillItPossible seed : " + seed);

    final Random random = new Random(seed);
    final Set<byte[]> addedKeys = new HashSet<>();

    addKeys(filter, random, addedKeys);

    checkFilterConsistency(filter, random, addedKeys);
  }

  private void checkFilterConsistency(OCuckooFilter filter, Random random, Set<byte[]> addedKeys) {
    for (final byte[] akey : addedKeys) {
      Assert.assertTrue(filter.contains(akey));
    }

    for (int i = 0; i < addedKeys.size() * 100; ) {
      byte[] akey = new byte[20];
      random.nextBytes(akey);

      if (!filter.contains(akey)) {
        Assert.assertTrue(!addedKeys.contains(akey));
        i++;
      }

    }
  }

  public void addItemsThenRemoveThemAndAddAgain() {
    int capacity = 1 << 10;
    OCuckooFilter filter = new OCuckooFilter(capacity);

    final long seed = System.currentTimeMillis();
    System.out.println("addItemsThenRemoveThemAndAddAgain seed : " + seed);

    final Random random = new Random(seed);
    final Set<byte[]> addedKeys = new HashSet<>();

    addKeys(filter, random, addedKeys);

    List<byte[]> toRemove = new ArrayList<>();
    for (byte[] k : addedKeys) {
      if (random.nextBoolean())
        toRemove.add(k);
    }

    for (byte[] d : toRemove) {
      addedKeys.remove(d);
      filter.remove(d);
    }

    checkFilterConsistency(filter, random, addedKeys);

    addKeys(filter, random, addedKeys);

    checkFilterConsistency(filter, random, addedKeys);
  }

  private void addKeys(OCuckooFilter filter, Random random, Set<byte[]> addedKeys) {
    byte[] key;
    while (true) {
      key = new byte[20];
      random.nextBytes(key);
      if (addedKeys.add(key)) {
        if (!filter.add(key))
          break;
      }
    }

    addedKeys.remove(key);
  }
}
