package com.orientechnologies.orient.core.index.lsmtree;

import com.orientechnologies.common.hash.OMurmurHash3;

import java.util.Random;

public class OCuckooFilter {
  private static final int[] RANDOM_NUMBERS;
  private static final int   SEED                 = 362498820;
  private static final float EXPECTED_LOAD_FACTOR = 0.9f;

  static {
    int[] rnds = new int[16];
    Random rnd = new Random(SEED);

    for (int i = 0; i < rnds.length; i++) {
      rnds[i] = rnd.nextInt();
    }

    RANDOM_NUMBERS = rnds;
  }

  private OCuckooArray firstArray;
  private OCuckooArray secondArray;

  private int size;

  public OCuckooFilter(int capacity) {
    final int arrayCapacity = (int) Math.ceil((capacity << 1) * EXPECTED_LOAD_FACTOR);

    firstArray = new OCuckooArray(arrayCapacity);
    secondArray = new OCuckooArray(arrayCapacity);
  }

  public boolean add(byte[] key) {
    final int[] result = fingerprintFirstSecondIndex(key);

    final int fingerprint = result[0];
    final int firstIndex = result[1];
    final int secondIndex = result[2];

    if (set(firstIndex, secondIndex, fingerprint)) {
      size++;

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

    int f = firstArray.get(firstIndex);
    if (f == fingerprint)
      return true;

    f = secondArray.get(secondIndex);

    return f == fingerprint;
  }

  private int[] fingerprintFirstSecondIndex(byte[] key) {
    final long h = OMurmurHash3.murmurHash3_x64_64(key, SEED);

    final int[] result = new int[3];

    result[0] = (int) (h & 0x0F);
    result[1] = firstArray.index((int) ((h >>> 4) & 0x7FFFFFFF));
    result[2] = result[1] ^ secondArray.index(jswHashing(result[0]));

    return result;
  }

  private boolean set(int firstIndex, int secondIndex, int fingerprint) {
    boolean inserted;

    inserted = firstArray.set(firstIndex, fingerprint);
    if (!inserted) {
      inserted = secondArray.set(secondIndex, fingerprint);
    }

    if (!inserted) {
      final int existingFingerprint = firstArray.get(firstIndex);

      final int nextIndex = secondArray.index(jswHashing(existingFingerprint)) ^ firstIndex;

      final boolean result = move(existingFingerprint, nextIndex, false, 0);
      if (result) {
        firstArray.remove(firstIndex, existingFingerprint);
        return set(firstIndex, secondIndex, fingerprint);
      } else {
        return false;
      }
    } else {

      return true;
    }
  }

  private boolean move(int fingerPrint, int index, boolean first, int counter) {
    if (counter > 500)
      return false;

    final boolean result;
    if (first) {
      if (firstArray.set(index, fingerPrint))
        return true;

      final int nextFingerprint = firstArray.get(index);
      final int nextIndex = secondArray.index(jswHashing(nextFingerprint)) ^ index;

      result = move(nextFingerprint, nextIndex, false, counter + 1);
      if (result) {
        firstArray.remove(index, nextFingerprint);
        firstArray.set(index, fingerPrint);
      }

      return result;
    } else {
      if (secondArray.set(index, fingerPrint))
        return true;

      final int nextFingerprint = secondArray.get(index);
      final int nextIndex = firstArray.index(jswHashing(nextFingerprint)) ^ index;

      result = move(nextFingerprint, nextIndex, true, counter + 1);
      if (result) {
        secondArray.remove(index, nextFingerprint);
        secondArray.set(index, fingerPrint);
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
      size--;
      return;
    }

    if (secondArray.remove(secondIndex, fingerprint)) {
      size--;
    }
  }

  public void clear() {
    firstArray.clear();
    secondArray.clear();
  }

  private int jswHashing(int fingerPrint) {
    final int h = 16777551;

    return ((h << 1) | (h >>> 31)) ^ RANDOM_NUMBERS[fingerPrint];
  }

}
