package com.orientechnologies.orient.core.index.lsmtree.cuckoofilter;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCuckooArrayException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;

/**
 * Array which will contain element of {@link OCuckooFilter}.
 * <p>
 * Each array bucket consist of 4 items. Each bucket has unique index but item itself does not have index.
 * Each item size is 16 bits or 2 bytes.
 */
public class OCuckooArray extends ODurableComponent {
  public static final String DEF_EXTENSION    = ".dca";
  public static final String FILLED_EXTENSION = ".fca";
  public static final String STATE_EXTENSION  = ".sca";

  private static final int DISK_PAGE_SIZE   = DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  private static final int USEFUL_PAGE_SIZE = closestPowerOfTwo((DISK_PAGE_SIZE - ODurablePage.NEXT_FREE_POSITION + 1) / 2);

  private static final int ITEM_SIZE_IN_BYTES = 2;
  private static final int ITEM_SIZE_IN_BITS  = 16;
  private static final int ITEMS_PER_BUCKET   = 4;
  private static final int FILL_BIT_MASK      = 0xF;

  private static int DATA_PAGE_SIZE =
      ((USEFUL_PAGE_SIZE + ITEM_SIZE_IN_BYTES * ITEMS_PER_BUCKET - 1) / (ITEM_SIZE_IN_BYTES * ITEMS_PER_BUCKET))
          * ITEM_SIZE_IN_BYTES * ITEMS_PER_BUCKET;

  private static int FILLED_PAGE_SIZE = USEFUL_PAGE_SIZE;

  private static final int ITEMS_PER_FILLED_PAGE = FILLED_PAGE_SIZE * 8;

  private static final int ITEMS_PER_DATA_PAGE = DATA_PAGE_SIZE / ITEM_SIZE_IN_BYTES;

  private static final int  MINIMUM_CAPACITY  = Math.max(ITEMS_PER_FILLED_PAGE, ITEMS_PER_DATA_PAGE);
  private static final long FINGER_PRINT_MASK = 0xFFFF;

  private int capacity;

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

  private long filledFileId;
  private long dataFileId;

  OCuckooArray(final String name, final String lockName, final OAbstractPaginatedStorage storage) {
    super(storage, name, DEF_EXTENSION, lockName);
  }

  public void open() throws IOException {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        filledFileId = openFile(atomicOperation, getName() + FILLED_EXTENSION);
        dataFileId = openFile(atomicOperation, getName() + DEF_EXTENSION);

        pinFilePages(atomicOperation, filledFileId);
        pinFilePages(atomicOperation, dataFileId);

        final long stateFileId = openFile(atomicOperation, getName() + STATE_EXTENSION);
        OCacheEntry cacheEntry = loadPage(atomicOperation, stateFileId, 0, false);
        try {
          cacheEntry.acquireSharedLock();
          try {
            final OStatePage statePage = new OStatePage(cacheEntry, getChanges(atomicOperation, cacheEntry));
            this.capacity = statePage.getCapacity();
          } finally {
            cacheEntry.releaseSharedLock();
          }
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  private void pinFilePages(OAtomicOperation atomicOperation, long fileId) throws IOException {
    final long filledSize = getFilledUpTo(atomicOperation, fileId);

    for (long filledPageIndex = 0; filledPageIndex < filledSize; filledPageIndex++) {
      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, filledPageIndex, false);
      try {
        pinPage(atomicOperation, cacheEntry);
      } finally {
        releasePage(atomicOperation, cacheEntry);
      }
    }
  }

  public void create(int capacity) throws IOException {
    if (capacity < MINIMUM_CAPACITY)
      capacity = MINIMUM_CAPACITY;

    capacity = closestPowerOfTwo(capacity);

    startOperation();
    try {
      acquireExclusiveLock();
      final OAtomicOperation atomicOperation = startAtomicOperation(false);
      try {
        filledFileId = addFile(atomicOperation, getName() + FILLED_EXTENSION);
        dataFileId = addFile(atomicOperation, getName() + DEF_EXTENSION);

        final long stateFileId = addFile(atomicOperation, getName() + STATE_EXTENSION);
        OCacheEntry cacheEntry = addPage(atomicOperation, stateFileId);
        try {
          cacheEntry.acquireExclusiveLock();
          try {
            final OStatePage statePage = new OStatePage(cacheEntry, getChanges(atomicOperation, cacheEntry));
            statePage.setCapacity(capacity);
          } finally {
            cacheEntry.releaseExclusiveLock();
          }
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }

        this.capacity = capacity;

        endAtomicOperation(false, null);
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OCuckooArrayException("Error during component creation", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }

  }

  void clear() {
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
  boolean add(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    long fillItem = filledTo[fillIndex];
    long fillBitMask = 1L << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

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

  boolean contains(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    long fillBitMask = 1L << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

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

  public int getBucketSize(int index) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    final int itemOffset = firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE;

    int fillBucketMask = (int) (FILL_BIT_MASK & (fillItem >>> itemOffset));
    return Integer.bitCount(fillBucketMask);
  }

  public int get(int index) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    long fillBitMask = 1L << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
      if ((fillItem & fillBitMask) != 0) {
        final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex + i - dataIndex * ITEMS_PER_DATA_PAGE);
        final long dataMask = FINGER_PRINT_MASK << dataOffset;

        return (int) ((item & dataMask) >>> dataOffset);
      }

      fillBitMask = fillBitMask << 1;
    }

    return -1;
  }

  public int get(int index, int itemIndex) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    final long fillItem = filledTo[fillIndex];
    long fillBitMask = 1L << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

    final int dataIndex = firstItemIndex / ITEMS_PER_DATA_PAGE;
    final long item = data[dataIndex];

    int ii = 0;
    for (int i = 0; i < ITEMS_PER_BUCKET; i++) {
      if ((fillItem & fillBitMask) != 0) {
        if (ii == itemIndex) {
          final int dataOffset = ITEM_SIZE_IN_BITS * (firstItemIndex + i - dataIndex * ITEMS_PER_DATA_PAGE);
          final long dataMask = FINGER_PRINT_MASK << dataOffset;

          return (int) ((item & dataMask) >>> dataOffset);
        }
      }

      ii++;
      fillBitMask = fillBitMask << 1;
    }

    return -1;
  }

  public boolean remove(int index, int fingerprint) {
    final int firstItemIndex = index * ITEMS_PER_BUCKET;
    final int fillIndex = firstItemIndex / ITEMS_PER_FILLED_PAGE;

    long fillItem = filledTo[fillIndex];
    long fillBitMask = 1L << (firstItemIndex - fillIndex * ITEMS_PER_FILLED_PAGE);

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

  int bucketIndex(int hash) {
    return (hash & 0x7FFFFFFF) % ((data.length * (DATA_PAGE_SIZE / ITEM_SIZE_IN_BYTES) / ITEMS_PER_BUCKET));
  }

  String printDebug() {
    int counter = 0;
    StringBuilder builder = new StringBuilder();

    for (long fillItem : filledTo) {
      int i = 0;
      while (i < USEFUL_PAGE_SIZE * 8) {
        final int bucketIndex = counter / ITEMS_PER_BUCKET;
        StringBuilder bucket = new StringBuilder();
        boolean notEmpty = false;

        bucket.append("{");
        for (int n = 0; n < ITEMS_PER_BUCKET; n++) {
          long fillMask = (1L << i);
          if ((fillItem & fillMask) != 0) {
            final int itemIndex = counter / ITEMS_PER_DATA_PAGE;
            final long dataOffset = ITEM_SIZE_IN_BITS * (counter - itemIndex * ITEMS_PER_DATA_PAGE);
            bucket.append("[").append(n).append(":").append((data[itemIndex] & (FINGER_PRINT_MASK << dataOffset)) >>> dataOffset).
                append("]");
            notEmpty = true;
          }

          counter++;
          i++;

        }
        bucket.append("}");

        if (notEmpty)
          builder.append("B : ").append(bucketIndex).append(" - ").append(bucket).append(" ");
      }
    }

    return builder.toString();
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most significant bit to the lowest bits for
   * the smallest number of iterations possible and then increment result value by 1.
   *
   * @param value Integer the most significant power of 2 should be found.
   * @return The most significant power of 2.
   */
  private static int closestPowerOfTwo(int value) {
    int n = value - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= (1 << 30)) ? 1 << 30 : n + 1;
  }

}
