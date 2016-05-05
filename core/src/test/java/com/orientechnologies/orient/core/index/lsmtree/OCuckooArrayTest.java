package com.orientechnologies.orient.core.index.lsmtree;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OCuckooArrayTest {

  public void testSingleItem() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);
    cuckooArray.set(5, 14);

    Assert.assertEquals(cuckooArray.get(5), 14);
    Assert.assertEquals(cuckooArray.get(6), -1);

    Assert.assertTrue(!cuckooArray.remove(5, 11));

    Assert.assertTrue(!cuckooArray.remove(4, 14));
    Assert.assertTrue(cuckooArray.remove(5, 14));

    Assert.assertEquals(cuckooArray.get(5), -1);
  }

  public void testAdjacentItems() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    cuckooArray.set(5, 1);
    Assert.assertEquals(cuckooArray.get(5), 1);

    cuckooArray.set(6, 5);
    Assert.assertEquals(cuckooArray.get(5), 1);
    Assert.assertEquals(cuckooArray.get(6), 5);

    cuckooArray.set(7, 11);

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

    Assert.assertTrue(!cuckooArray.remove(7, 11));

    Assert.assertEquals(cuckooArray.get(5), -1);
    Assert.assertEquals(cuckooArray.get(6), -1);
    Assert.assertEquals(cuckooArray.get(7), -1);
  }
}
