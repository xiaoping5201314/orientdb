package com.orientechnologies.orient.core.index.lsmtree;

public class OCuckooArray {
  private long[] filledTo;
  private int[]  data;

  public OCuckooArray(int capacity) {
    data = new int[(capacity + 7) >>> 3];
    filledTo = new long[(capacity + 63) >>> 6];
  }

  public void clear() {
    data = new int[data.length];
  }

  public boolean set(int hash, int fingerprint) {
    final int index = index(hash);
    final int fillIndex = index >>> 6;

    long fillItem = filledTo[fillIndex];
    final int fillBitMask = 1 << (index - fillIndex);

    if ((fillItem & fillBitMask) > 0)
      return false;

    fillItem = fillItem | fillBitMask;
    filledTo[fillIndex] = fillItem;

    final int itemIndex = index >>> 3;
    int item = data[itemIndex];

    final int dataOffset = 4 * (index - itemIndex);
    final int dataMask = 0xFF << dataOffset;

    data[itemIndex] = ((item & dataMask) & ~dataMask) | (fingerprint << dataOffset);

    return true;
  }

  public int get(int hash) {
    final int index = index(hash);
    final int fillIndex = index >>> 6;

    final long fillItem = filledTo[fillIndex];
    final int fillBitMask = 1 << (index - fillIndex);

    if ((fillItem & fillBitMask) == 0)
      return -1;

    final int itemIndex = index >>> 3;
    final int item = data[itemIndex];

    final int dataOffset = 4 * (index - itemIndex);
    final int dataMask = 0xFF << dataOffset;

    return (item & dataMask) >>> dataOffset;
  }

  public boolean remove(int hash, int fingerprint) {
    final int index = index(hash);
    final int fillIndex = index >>> 6;

    long fillItem = filledTo[fillIndex];
    final int fillBitMask = 1 << (index - fillIndex);

    if ((fillItem & fillBitMask) == 0)
      return false;

    final int itemIndex = index >>> 3;
    final int item = data[itemIndex];

    final int offset = (4 * (index - itemIndex));
    final int dataMask = 0xFF << offset;

    if ((item & dataMask) != (fingerprint << offset))
      return false;

    fillItem = fillItem & (~fillBitMask);
    filledTo[fillIndex] = fillItem;

    return true;
  }

  private int index(int hash) {
    return hash % (data.length << 3);
  }
}
