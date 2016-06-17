package com.orientechnologies.orient.core.index.lsmtree.cuckoofilter;

import com.orientechnologies.common.hash.OMurmurHash3;

import java.util.Random;

public class OCuckooFilter {
  private static final int[] RANDOM_NUMBERS;
  private static final int SEED = 362498820;

  static {
    int[] rnds = new int[256];
    Random rnd = new Random(SEED);

    for (int i = 0; i < rnds.length; i++) {
      rnds[i] = rnd.nextInt();
    }

    RANDOM_NUMBERS = rnds;
  }

  private OCuckooArray firstArray;
  private OCuckooArray secondArray;

  public OCuckooFilter(int capacity) {
    final int arrayCapacity = capacity >>> 1;

    firstArray = new OCuckooArray(arrayCapacity);
    secondArray = new OCuckooArray(arrayCapacity);
  }

  public boolean add(byte[] key) {
    final int[] result = fingerprintFirstSecondIndex(key);

    final int fingerprint = result[0];
    final int firstIndex = result[1];
    final int secondIndex = result[2];

    if (set(firstIndex, secondIndex, fingerprint)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean contains(byte[] key) {
    final int[] result = fingerprintFirstSecondIndex(key);

    final int fingerprint = result[0];
    final int firstIndex = result[1];
    final int secondIndex = result[2];

    if (firstArray.contains(firstIndex, fingerprint))
      return true;

    return secondArray.contains(secondIndex, fingerprint);
  }

  private int[] fingerprintFirstSecondIndex(byte[] key) {
    final long h = OMurmurHash3.murmurHash3_x64_64(key, SEED);

    final int[] result = new int[3];

    result[0] = (int) (h & 0xFFFF);
    result[1] = firstArray.bucketIndex((int) ((h >>> 16) & 0x7FFFFFFF));
    result[2] = result[1] ^ secondArray.bucketIndex(jswHashing(result[0]));

    return result;
  }

  private boolean set(int firstIndex, int secondIndex, int fingerprint) {
    boolean inserted;

    inserted = firstArray.add(firstIndex, fingerprint);
    if (!inserted) {
      inserted = secondArray.add(secondIndex, fingerprint);
    }

    if (!inserted) {
      boolean result = false;
      int existingFingerprint = -1;
      for (int bucketSize = firstArray.getBucketSize(firstIndex), n = 0; n < bucketSize && !result; n++) {
        existingFingerprint = firstArray.get(firstIndex, n);
        int nextIndex = secondArray.bucketIndex(jswHashing(existingFingerprint)) ^ firstIndex;
        result = move(existingFingerprint, nextIndex, false, 0);
      }
      if (result) {
        firstArray.remove(firstIndex, existingFingerprint);
        return set(firstIndex, secondIndex, fingerprint);
      } else {
        result = false;

        for (int bucketSize = secondArray.get(secondIndex), n = 0; n < bucketSize && !result; n++) {
          existingFingerprint = secondArray.get(secondIndex);
          int nextIndex = secondArray.bucketIndex(jswHashing(existingFingerprint)) ^ secondIndex;
          result = move(existingFingerprint, nextIndex, true, 0);
        }

        if (result) {
          secondArray.remove(secondIndex, existingFingerprint);
          return set(firstIndex, secondIndex, fingerprint);
        }
      }
      return false;
    } else {

      return true;
    }
  }

  private boolean move(int fingerPrint, int index, boolean first, int counter) {
    if (counter > 500)
      return false;

    final boolean result;
    if (first) {
      if (firstArray.add(index, fingerPrint))
        return true;

      final int nextFingerprint = firstArray.get(index);
      final int nextIndex = secondArray.bucketIndex(jswHashing(nextFingerprint)) ^ index;

      result = move(nextFingerprint, nextIndex, false, counter + 1);
      if (result) {
        firstArray.remove(index, nextFingerprint);
        firstArray.add(index, fingerPrint);
      }

      return result;
    } else {
      if (secondArray.add(index, fingerPrint))
        return true;

      final int nextFingerprint = secondArray.get(index);
      final int nextIndex = firstArray.bucketIndex(jswHashing(nextFingerprint)) ^ index;

      result = move(nextFingerprint, nextIndex, true, counter + 1);
      if (result) {
        secondArray.remove(index, nextFingerprint);
        secondArray.add(index, fingerPrint);
      }

      return result;
    }
  }

  public void remove(byte[] key) {
    final int[] result = fingerprintFirstSecondIndex(key);

    final int fingerprint = result[0];
    final int firstIndex = result[1];
    final int secondIndex = result[2];

    if (firstArray.remove(firstIndex, fingerprint)) {
      return;
    }

    secondArray.remove(secondIndex, fingerprint);
  }

  public void clear() {
    firstArray.clear();
    secondArray.clear();
  }

  public String printDebug() {
    StringBuilder builder = new StringBuilder();
    builder.append("List 1 ").append(firstArray.printDebug()).append("\n");
    builder.append("List 2 ").append(secondArray.printDebug());

    return builder.toString();
  }

  private int jswHashing(int fingerPrint) {
    int h = 16777551;

    for (int i = 0; i < 2; i++) {
      h = (h << 1 | h >> 31) ^ RANDOM_NUMBERS[0xFF & (fingerPrint >> i)];
    }

    return h;
  }

}
