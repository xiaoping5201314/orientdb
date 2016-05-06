package com.orientechnologies.orient.core.index.lsmtree;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OCuckooArrayTest {

  public void testSingleItem() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    Assert.assertTrue(cuckooArray.set(5, 14));
    Assert.assertTrue(!cuckooArray.set(5, 14));

    Assert.assertEquals(cuckooArray.get(5), 14);
    Assert.assertEquals(cuckooArray.get(6), -1);

    Assert.assertTrue(!cuckooArray.remove(5, 11));

    Assert.assertTrue(!cuckooArray.remove(4, 14));
    Assert.assertTrue(cuckooArray.remove(5, 14));

    Assert.assertEquals(cuckooArray.get(5), -1);

    Assert.assertTrue(cuckooArray.set(5, 15));
    Assert.assertTrue(!cuckooArray.set(5, 15));

    Assert.assertEquals(cuckooArray.get(5), 15);
  }

  public void testAdjacentItems() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    Assert.assertTrue(cuckooArray.set(5, 1));
    Assert.assertTrue(!cuckooArray.set(5, 1));

    Assert.assertEquals(cuckooArray.get(5), 1);

    Assert.assertTrue(cuckooArray.set(6, 5));
    Assert.assertTrue(!cuckooArray.set(6, 5));

    Assert.assertEquals(cuckooArray.get(5), 1);
    Assert.assertEquals(cuckooArray.get(6), 5);

    Assert.assertTrue(cuckooArray.set(7, 11));
    Assert.assertTrue(!cuckooArray.set(7, 11));

    Assert.assertEquals(cuckooArray.get(5), 1);
    Assert.assertEquals(cuckooArray.get(6), 5);
    Assert.assertEquals(cuckooArray.get(7), 11);

    Assert.assertTrue(!cuckooArray.remove(5, 11));
    Assert.assertTrue(!cuckooArray.remove(6, 1));
    Assert.assertTrue(!cuckooArray.remove(7, 5));

    Assert.assertTrue(!cuckooArray.remove(4, 1));
    Assert.assertTrue(!cuckooArray.remove(3, 5));
    Assert.assertTrue(!cuckooArray.remove(2, 11));

    Assert.assertTrue(cuckooArray.remove(5, 1));

    Assert.assertEquals(cuckooArray.get(5), -1);
    Assert.assertEquals(cuckooArray.get(6), 5);
    Assert.assertEquals(cuckooArray.get(7), 11);

    Assert.assertTrue(cuckooArray.remove(6, 5));

    Assert.assertEquals(cuckooArray.get(5), -1);
    Assert.assertEquals(cuckooArray.get(6), -1);
    Assert.assertEquals(cuckooArray.get(7), 11);

    Assert.assertTrue(cuckooArray.remove(7, 11));

    Assert.assertEquals(cuckooArray.get(5), -1);
    Assert.assertEquals(cuckooArray.get(6), -1);
    Assert.assertEquals(cuckooArray.get(7), -1);

    Assert.assertTrue(cuckooArray.set(5, 2));
    Assert.assertTrue(cuckooArray.set(6, 6));
    Assert.assertTrue(cuckooArray.set(7, 12));

    Assert.assertEquals(cuckooArray.get(5), 2);
    Assert.assertEquals(cuckooArray.get(6), 6);
    Assert.assertEquals(cuckooArray.get(7), 12);

    Assert.assertTrue(!cuckooArray.set(5, 2));
    Assert.assertTrue(!cuckooArray.set(6, 6));
    Assert.assertTrue(!cuckooArray.set(7, 12));
  }

  public void testSingleItemNextPage() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);
    Assert.assertTrue(cuckooArray.set(10, 14));
    Assert.assertTrue(!cuckooArray.set(10, 14));

    Assert.assertEquals(cuckooArray.get(10), 14);
    Assert.assertEquals(cuckooArray.get(9), -1);

    Assert.assertTrue(!cuckooArray.remove(10, 11));

    Assert.assertTrue(!cuckooArray.remove(9, 14));
    Assert.assertTrue(cuckooArray.remove(10, 14));

    Assert.assertEquals(cuckooArray.get(10), -1);

    Assert.assertTrue(cuckooArray.set(10, 15));
    Assert.assertEquals(cuckooArray.get(10), 15);
    Assert.assertTrue(!cuckooArray.set(10, 15));
  }

  public void testAdjacentItemsInSeveralPages() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    Assert.assertTrue(cuckooArray.set(10, 1));
    Assert.assertTrue(!cuckooArray.set(10, 1));
    Assert.assertEquals(cuckooArray.get(10), 1);

    Assert.assertTrue(cuckooArray.set(9, 5));
    Assert.assertTrue(!cuckooArray.set(9, 5));

    Assert.assertEquals(cuckooArray.get(10), 1);
    Assert.assertEquals(cuckooArray.get(9), 5);

    Assert.assertTrue(cuckooArray.set(8, 11));
    Assert.assertTrue(!cuckooArray.set(8, 11));

    Assert.assertEquals(cuckooArray.get(10), 1);
    Assert.assertEquals(cuckooArray.get(9), 5);
    Assert.assertEquals(cuckooArray.get(8), 11);

    Assert.assertTrue(!cuckooArray.remove(10, 11));
    Assert.assertTrue(!cuckooArray.remove(9, 1));
    Assert.assertTrue(!cuckooArray.remove(8, 5));

    Assert.assertTrue(!cuckooArray.remove(7, 1));
    Assert.assertTrue(!cuckooArray.remove(5, 5));
    Assert.assertTrue(!cuckooArray.remove(5, 11));

    Assert.assertTrue(cuckooArray.remove(10, 1));

    Assert.assertEquals(cuckooArray.get(10), -1);
    Assert.assertEquals(cuckooArray.get(9), 5);
    Assert.assertEquals(cuckooArray.get(8), 11);

    Assert.assertTrue(cuckooArray.remove(9, 5));

    Assert.assertEquals(cuckooArray.get(10), -1);
    Assert.assertEquals(cuckooArray.get(9), -1);
    Assert.assertEquals(cuckooArray.get(8), 11);

    Assert.assertTrue(cuckooArray.remove(8, 11));

    Assert.assertEquals(cuckooArray.get(10), -1);
    Assert.assertEquals(cuckooArray.get(9), -1);
    Assert.assertEquals(cuckooArray.get(8), -1);

    Assert.assertTrue(cuckooArray.set(10, 2));
    Assert.assertTrue(cuckooArray.set(9, 6));
    Assert.assertTrue(cuckooArray.set(8, 12));

    Assert.assertEquals(cuckooArray.get(10), 2);
    Assert.assertEquals(cuckooArray.get(9), 6);
    Assert.assertEquals(cuckooArray.get(8), 12);

    Assert.assertTrue(!cuckooArray.set(10, 2));
    Assert.assertTrue(!cuckooArray.set(9, 6));
    Assert.assertTrue(!cuckooArray.set(8, 12));
  }

  public void testClear() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    cuckooArray.set(10, 1);
    cuckooArray.set(9, 5);
    cuckooArray.set(8, 11);

    cuckooArray.clear();

    Assert.assertEquals(cuckooArray.get(10), -1);
    Assert.assertEquals(cuckooArray.get(9), -1);
    Assert.assertEquals(cuckooArray.get(8), -1);

    Assert.assertTrue(cuckooArray.set(10, 2));
    Assert.assertTrue(cuckooArray.set(9, 6));
    Assert.assertTrue(cuckooArray.set(8, 12));

    Assert.assertEquals(cuckooArray.get(10), 2);
    Assert.assertEquals(cuckooArray.get(9), 6);
    Assert.assertEquals(cuckooArray.get(8), 12);
  }
}
