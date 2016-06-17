package com.orientechnologies.orient.core.index.lsmtree;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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

    System.out.println("addItemsTillItPossible capacity :" + capacity);
    OCuckooFilter filter = new OCuckooFilter(capacity);

    final long seed = System.currentTimeMillis();
    System.out.println("addItemsTillItPossible seed : " + seed);

    final Random random = new Random(seed);
    final Set<byte[]> addedKeys = new HashSet<>();

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
}
