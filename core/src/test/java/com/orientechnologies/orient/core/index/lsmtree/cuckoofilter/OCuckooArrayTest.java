package com.orientechnologies.orient.core.index.lsmtree.cuckoofilter;

import com.orientechnologies.orient.core.index.lsmtree.cuckoofilter.OCuckooArray;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.function.UnaryOperator;

@Test
public class OCuckooArrayTest {

  public void testSingleItem() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    Assert.assertEquals(cuckooArray.getBucketSize(5), 0);
    Assert.assertTrue(cuckooArray.add(5, 14));
    Assert.assertEquals(cuckooArray.getBucketSize(5), 1);
    Assert.assertTrue(cuckooArray.contains(5, 14));
    Assert.assertEquals(cuckooArray.get(5), 14);
    Assert.assertEquals(cuckooArray.get(5, 0), 14);
    Assert.assertEquals(cuckooArray.get(5, 1), -1);

    Assert.assertTrue(!cuckooArray.contains(6, 14));
    Assert.assertEquals(cuckooArray.getBucketSize(6), 0);

    Assert.assertTrue(!cuckooArray.remove(5, 11));
    Assert.assertTrue(!cuckooArray.remove(4, 14));

    Assert.assertTrue(cuckooArray.remove(5, 14));
    Assert.assertEquals(cuckooArray.get(5), -1);
    Assert.assertEquals(cuckooArray.get(5, 0), -1);
    Assert.assertEquals(cuckooArray.getBucketSize(5), 0);

    Assert.assertTrue(!cuckooArray.contains(5, 14));

    Assert.assertTrue(cuckooArray.add(5, 15));
    Assert.assertEquals(cuckooArray.get(5, 0), 15);
    Assert.assertTrue(cuckooArray.contains(5, 15));
    Assert.assertEquals(cuckooArray.get(5), 15);
    Assert.assertEquals(cuckooArray.getBucketSize(5), 1);
  }

  public void test4ItemsOneBucket() {
    final OCuckooArray cuckooArray = new OCuckooArray(10);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(1, 5 + i));
    }

    Assert.assertEquals(cuckooArray.get(1), -1);

    for (int i = 0; i < 4; i++) {
      final int k = i;
      Assert.assertEquals(cuckooArray.getBucketSize(1), i);
      assertBucketContent(cuckooArray, 1, populateContent((n) -> {
        if (n < k)
          return 5 + n;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.add(1, 5 + i));

      assertBucketContent(cuckooArray, 1, populateContent((n) -> {
        if (n <= k)
          return 5 + n;
        return -1;
      }));
      Assert.assertEquals(cuckooArray.getBucketSize(1), i + 1);
    }

    Assert.assertEquals(cuckooArray.get(1), 5);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.add(1, i));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(1, i));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(1, 5 + i));
    }

    for (int i = 0; i < 4; i++) {
      final int k = i;

      Assert.assertEquals(cuckooArray.getBucketSize(1), 4 - i);

      assertBucketContent(cuckooArray, 1, populateContent((n) -> {
        if (n >= k)
          return 5 + n;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.remove(1, 5 + i));

      assertBucketContent(cuckooArray, 1, populateContent((n) -> {
        if (n > k)
          return 5 + n;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(1), 3 - i);

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
    final OCuckooArray cuckooArray = new OCuckooArray(260);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(63, 5 + i));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.contains(64, i + 1));
    }

    Assert.assertEquals(cuckooArray.get(63), -1);
    Assert.assertEquals(cuckooArray.get(64), -1);

    for (int i = 0; i < 4; i++) {
      final int k = i;
      Assert.assertEquals(cuckooArray.getBucketSize(63), i);

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n < k)
          return 5 + n;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.add(63, 5 + i));

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n <= k)
          return 5 + n;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(63), i + 1);
    }

    for (int i = 0; i < 4; i++) {
      final int k = i;
      Assert.assertEquals(cuckooArray.getBucketSize(64), i);

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n < k)
          return n + 1;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.add(64, i + 1));

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n <= k)
          return n + 1;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(64), i + 1);
    }

    Assert.assertEquals(cuckooArray.get(63), 5);
    Assert.assertEquals(cuckooArray.get(64), 1);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.add(63, i + 1));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.add(64, i + 5));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(63, i + 1));
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(!cuckooArray.remove(64, i + 5));
    }

    for (int i = 0; i < 4; i++) {
      final int k = i;

      Assert.assertEquals(cuckooArray.getBucketSize(63), 4 - i);

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n >= k)
          return n + 5;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.remove(63, i + 5));

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n > k)
          return n + 5;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(63), 3 - i);

      if (i < 3) {
        Assert.assertEquals(cuckooArray.get(63), i + 5 + 1);
      }
    }

    Assert.assertEquals(cuckooArray.get(63), -1);

    for (int i = 0; i < 4; i++) {
      final int k = i;

      Assert.assertEquals(cuckooArray.getBucketSize(64), 4 - i);

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n >= k)
          return n + 1;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.remove(64, i + 1));

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n > k)
          return n + 1;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(64), 3 - i);

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
      final int k = i;

      Assert.assertEquals(cuckooArray.getBucketSize(63), i);

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n < k)
          return n + 1;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.add(63, i + 1));

      assertBucketContent(cuckooArray, 63, populateContent((n) -> {
        if (n <= k)
          return n + 1;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(63), i + 1);
    }

    for (int i = 0; i < 4; i++) {
      final int k = i;

      Assert.assertEquals(cuckooArray.getBucketSize(64), i);

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n < k)
          return n + 5;
        return -1;
      }));

      Assert.assertTrue(cuckooArray.add(64, i + 5));

      assertBucketContent(cuckooArray, 64, populateContent((n) -> {
        if (n <= k)
          return n + 5;
        return -1;
      }));

      Assert.assertEquals(cuckooArray.getBucketSize(64), i + 1);
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(63, i + 1));
    }
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(cuckooArray.contains(64, i + 5));
    }
  }

  private void assertBucketContent(OCuckooArray array, int bucket, int[] content) {
    for (int i = 0; i < content.length; i++) {
      Assert.assertEquals(array.get(bucket, i), content[i]);
    }
  }

  private int[] populateContent(UnaryOperator<Integer> supplyer) {
    int[] content = new int[4];
    for (int i = 0; i < 4; i++) {
      content[i] = supplyer.apply(i);
    }

    return content;
  }
}
