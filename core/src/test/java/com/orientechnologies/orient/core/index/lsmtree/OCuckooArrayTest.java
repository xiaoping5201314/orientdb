package com.orientechnologies.orient.core.index.lsmtree;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OCuckooArrayTest {

  public void testSingleItem() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    Assert.assertTrue(cuckooArray.set(5, 14));
    Assert.assertTrue(cuckooArray.contains(5, 14));
    Assert.assertEquals(cuckooArray.get(5), 14);

    Assert.assertTrue(!cuckooArray.contains(6, 14));

    Assert.assertTrue(!cuckooArray.remove(5, 11));
    Assert.assertTrue(!cuckooArray.remove(4, 14));

    Assert.assertTrue(cuckooArray.remove(5, 14));
    Assert.assertEquals(cuckooArray.get(5), -1);

    Assert.assertTrue(!cuckooArray.contains(5, 14));

    Assert.assertTrue(cuckooArray.set(5, 15));
    Assert.assertTrue(cuckooArray.contains(5, 15));
    Assert.assertEquals(cuckooArray.get(5), 15);
  }

  public void test4ItemsOneBucket() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(1, 5 + i));
    }

    Assert.assertEquals(cuckooArray.get(1), -1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.set(1, 5 + i));
    }

    Assert.assertEquals(cuckooArray.get(1), 5);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.set(1, i));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(1, i));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(1, 5 + i));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.remove(1, 5 + i));

      if (i < 3) {
        Assert.assertEquals(cuckooArray.get(1), 5 + i + 1);
      }
    }

    Assert.assertEquals(cuckooArray.get(1), -1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(1, 5 + i));
    }
  }

  public void test8ItemsTwoBuckets() {
    final OCuckooArray cuckooArray = new OCuckooArray(128);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(63, 5 + i));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(64, i + 1));
    }

    Assert.assertEquals(cuckooArray.get(63), -1);
    Assert.assertEquals(cuckooArray.get(64), -1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.set(63, 5 + i));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.set(64, i + 1));
    }

    Assert.assertEquals(cuckooArray.get(63), 5);
    Assert.assertEquals(cuckooArray.get(64), 1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.set(63, i + 1));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.set(64, i + 5));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(63, i + 1));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(64, i + 5));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.remove(63, i + 5));

      if (i < 3) {
        Assert.assertEquals(cuckooArray.get(63), i + 5 + 1);
      }
    }

    Assert.assertEquals(cuckooArray.get(63), -1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.remove(64, i + 1));

      if (i < 3) {
        Assert.assertEquals(cuckooArray.get(64), i + 1 + 1);
      }
    }

    Assert.assertEquals(cuckooArray.get(64), -1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(63, 5 + i));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(64, i + 1));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.set(63, i + 1));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.set(64, i + 5));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(63, i + 1));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(64, i + 5));
    }
  }
}
