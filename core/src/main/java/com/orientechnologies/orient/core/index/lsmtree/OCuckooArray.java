package com.orientechnologies.orient.core.index.lsmtree;

public class OCuckooArray {
  private long[] filledTo;
  private int[]  data;

  public OCuckooArray(int capacity) {
    if (capacity < 64)
      capacity = 64;

    capacity = closestPowerOfTwo(capacity);

    data = new int[capacity >>> 1];
    filledTo = new long[capacity >>> 4];
  }

  public void clear() {
    for (int i = 0; i < filledTo.length; i++) {
      filledTo[i] = 0;
    }
  }

  public boolean set(int index, int fingerprint) {
    final int fillIndex = index >>> (6 - 2);//4 items per bucket

    long fillItem = filledTo[fillIndex];
    int fillBitMask = 1 << (index - fillIndex);

    int freeIndex = -1;
    for (int i = 0; i < 4; i++) {
      if ((fillItem & fillBitMask) == 0) {
        freeIndex = i;
        break;
      }

      fillBitMask = fillBitMask << 1;
    }

    if (freeIndex == -1)
      return false;

    fillItem = fillItem | fillBitMask;
    filledTo[fillIndex] = fillItem;

    final int itemIndex = (index >>> (3 - 2)) + freeIndex;
    int item = data[itemIndex];

    final int dataOffset = 4 * ((index << 2) - itemIndex);
    final int dataMask = 0xF << dataOffset;

    data[itemIndex] = (item & ~dataMask) | (fingerprint << dataOffset);

    return true;
  }

  public boolean contains(int index, int fingerprint) {
    final int fillIndex = index >>> 4;

    final long fillItem = filledTo[fillIndex];
    int fillBitMask = 1 << (index - fillIndex);

    for (int i = 0; i < 4; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int itemIndex = (index >>> 1) + i;
        final int item = data[itemIndex];

        final int dataOffset = 4 * ((index << 2) - itemIndex);
        final int dataMask = 0xF << dataOffset;

        if ((item & dataMask) == (fingerprint << dataOffset)) {
          return true;
        }
      }

      fillBitMask = fillBitMask << 1;
    }

    return false;
  }

  public int get(int index) {
    final int fillIndex = index >>> 4;

    final long fillItem = filledTo[fillIndex];
    int fillBitMask = 1 << (index - fillIndex);

    for (int i = 0; i < 4; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int itemIndex = (index >>> 1) + i;
        final int item = data[itemIndex];

        final int dataOffset = 4 * ((index << 2) - itemIndex);
        final int dataMask = 0xF << dataOffset;

        return (item & dataMask) >> dataOffset;
      }

      fillBitMask = fillBitMask << 1;
    }

    return -1;
  }

  public boolean remove(int index, int fingerprint) {
    final int fillIndex = index >>> 4;

    long fillItem = filledTo[fillIndex];
    int fillBitMask = 1 << (index - fillIndex);

    for (int i = 0; i < 4; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int itemIndex = (index >>> 1) + i;
        final int item = data[itemIndex];

        final int dataOffset = 4 * ((index << 2) - itemIndex);
        final int dataMask = 0xF << dataOffset;
        if ((item & dataMask) == (fingerprint << dataOffset)) {
          fillItem = fillItem & (~fillBitMask);
          filledTo[fillIndex] = fillItem;

          return true;
        }
      }

      fillBitMask = fillBitMask << 1;
    }

    return false;
  }

  public int index(int hash) {
    return (hash & 0x7FFFFFFF) & ((data.length << 5) - 1);
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most significant bit to the lowest bits for
   * the smallest number of iterations possible and then increment result value by 1.
   *
   * @param value Integer the most significant power of 2 should be found.
   * @return The most significant power of 2.
   */
  private int closestPowerOfTwo(int value) {
    int n = value - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= (1 << 30)) ? 1 << 30 : n + 1;
  }

}
