package com.orientechnologies.orient.core.index.lsmtree;

/**
 * Array which will contain element of {@link OCuckooFilter}.
 * <p>
 * Each array bucket consist of 4 items. Each bucket has unique index but item itself does not have index.
 * Each item size is 16 bits of 2 bytes.
 */
public class OCuckooArray {
  private static final int PAGE_SIZE = 8;

  private static final int ITEM_SIZE_IN_BYTES = 2;
  private static final int ITEM_SIZE_IN_BITS  = 16;
  private static final int ITEMS_PER_BUCKET   = 4;

  private static final int ITEMS_PER_FILLED_PAGE = PAGE_SIZE * 8;

  private static final int ITEMS_PER_DATA_PAGE = PAGE_SIZE / ITEM_SIZE_IN_BYTES;

  private static final int  MINIMUM_CAPACITY  = Math.max(ITEMS_PER_FILLED_PAGE, ITEMS_PER_DATA_PAGE);
  private static final long FINGER_PRINT_MASK = 0xFFFF;

  /**
   * List of bits each bit shows weather related item inside of bucket is filled or not.
   * Each bucket stores items in continuous manner (there will be no space between items inside of bucket).
   * So for example if we need to check whether there is at least one item in 2-nd bucket
   * we will examine bits from 4-th till 7.
   * All array of values is treated as continuous stripe of bits.
   * So if we want to check 34-th item, we need to check bits starting from 136 till 139 or bits from 0 till 3 of array item with
   * index 17.
   */
  private long[] filledTo;
  /**
   * Presentation of buckets. Because each item has 2 bytes, then each array item can contain 2 items.
   */
  private long[] data;

  public OCuckooArray(int capacity) {
    if (capacity < MINIMUM_CAPACITY)
      capacity = MINIMUM_CAPACITY;

    capacity = closestPowerOfTwo(capacity);

    data = new long[(capacity + ITEMS_PER_DATA_PAGE - 1) / ITEMS_PER_DATA_PAGE];
    filledTo = new long[(capacity + ITEMS_PER_FILLED_PAGE - 1) / ITEMS_PER_FILLED_PAGE];
  }

  public void clear() {
    for (int i = 0; i < filledTo.length; i++) {
      filledTo[i] = 0;
    }
  }

  /**
   * Adds fingerprint to bucket with given index.
   *
   * @param index       Index of bucket to add
   * @param fingerprint Fingerprint to add
   * @return <code>true</code> if bucket was not full and fingerprint was successfully added.
   */
  public boolean add(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    long fillItem = filledTo[fillIndex];
    long fillBitMask = 1 << (firstItemIndex - firstItemIndex * ITEMS_PER_FILLED_PAGE);

    int freeIndex = -1;
    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
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

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex - dataIndex * ITEMS_PER_DATA_PAGE + freeIndex);
    final long dataMask = FINGER_PRINT_MASK << dataOffset;

    data[dataIndex] = (item & ~dataMask) | (((long) fingerprint) << dataOffset);

    return true;
  }

  public boolean contains(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    long fillBitMask = 1 << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex + i - dataIndex * ITEMS_PER_DATA_PAGE);
        final long dataMask = FINGER_PRINT_MASK << dataOffset;

        if ((item & dataMask) == (((long) fingerprint) << dataOffset)) {
          return true;
        }
      }

      fillBitMask = fillBitMask << 1;
    }

    return false;
  }

  public int get(int index) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    long fillBitMask = 1 << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex + i - dataIndex * ITEMS_PER_DATA_PAGE);
        final long dataMask = FINGER_PRINT_MASK << dataOffset;

        return (int) ((item & dataMask) >> dataOffset);
      }

      fillBitMask = fillBitMask << 1;
    }

    return -1;
  }

  public boolean remove(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    long fillItem = filledTo[fillIndex];
    long fillBitMask = 1 << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
      if ((fillItem & fillBitMask) != 0) {

        final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex + i - dataIndex * ITEMS_PER_DATA_PAGE);
        final long dataMask = FINGER_PRINT_MASK << dataOffset;

        if ((item & dataMask) == (((long) fingerprint) << dataOffset)) {
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
