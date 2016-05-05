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
    final int[] result = fingerprintFirstSecondHash(key);

    final int fingerprint = result[0];
    final int firstHash = result[1];
    final int secondHash = result[2];

    boolean inserted = false;
    for (int i = 0; i < 500 && !inserted; i++) {
      inserted = firstArray.set(firstHash, fingerprint);
      if (!inserted) {
        inserted = secondArray.set(secondHash, fingerprint);
      }
    }

    if (inserted)
      return true;

    if (set(firstHash, secondHash, fingerprint)) {
      return add(key);
    } else {
      return false;
    }
  }

  public boolean contains(byte[] key) {
    final int[] result = fingerprintFirstSecondHash(key);

    final int fingerprint = result[0];
    final int firstHash = result[1];
    final int secondHash = result[2];

    int f = firstArray.get(firstHash);
    if (f == fingerprint)
      return true;

    f = secondArray.get(secondHash);

    return f == fingerprint;
  }

  private int[] fingerprintFirstSecondHash(byte[] key) {
    final long h = OMurmurHash3.murmurHash3_x64_64(key, SEED);

    final int[] result = new int[3];

    result[0] = (int) (h & 0x0F);
    result[1] = (int) ((h >>> 4) & 0x7FFFFFFF);
    result[2] = result[1] ^ jswHashing(result[2]);
    return result;
  }

  private boolean set(int firstHash, int secondHash, int fingerprint) {
    boolean inserted;

    inserted = firstArray.set(firstHash, fingerprint);
    if (!inserted) {
      inserted = secondArray.set(secondHash, fingerprint);
    }

    if (!inserted) {
      final int existingFingerprint = firstArray.get(firstHash);

      final int nextHash = jswHashing(existingFingerprint) ^ firstHash;

      if (move(existingFingerprint, nextHash, false, 0)) {
        return set(firstHash, secondHash, fingerprint);
      } else {
        return false;
      }
    } else {
      size++;

      return true;
    }
  }

  private boolean move(int fingerPrint, int hash, boolean first, int counter) {
    if (counter > 500)
      return false;

    if (first) {
      if (firstArray.set(fingerPrint, hash))
        return true;

      final int nextFingerprint = firstArray.get(hash);
      final int nextHash = jswHashing(nextFingerprint) ^ hash;

      return move(nextFingerprint, nextHash, false, counter + 1);
    } else {
      if (secondArray.set(fingerPrint, hash))
        return true;

      final int nextFingerprint = secondArray.get(hash);
      final int nextHash = jswHashing(nextFingerprint) ^ hash;

      return move(nextFingerprint, nextHash, true, counter + 1);
    }
  }

  public void remove(byte[] key) {
    final int[] result = fingerprintFirstSecondHash(key);

    final int fingerprint = result[0];
    final int firstHash = result[1];
    final int secondHash = result[2];

    if (firstArray.remove(firstHash, fingerprint)) {
      size--;
      return;
    }

    if (secondArray.remove(secondHash, fingerprint)) {
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
