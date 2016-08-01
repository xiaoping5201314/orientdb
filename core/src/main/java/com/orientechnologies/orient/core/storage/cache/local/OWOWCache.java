/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.common.collection.closabledictionary.OClosableEntry;
import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OWriteCacheException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OWOWCache extends OAbstractWriteCache implements OWriteCache, OCachePointer.WritersListener {
  // we add 8 bytes before and after cache pages to prevent word tearing in mt case.

  private int maxPagesPerFlush;

  public static final String NAME_ID_MAP_EXTENSION = ".cm";

  private static final String NAME_ID_MAP = "name_id_map" + NAME_ID_MAP_EXTENSION;

  public static final int MIN_CACHE_SIZE = 16;

  public static final long MAGIC_NUMBER = 0xFACB03FEL;

  private final long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024L * 1024L;

  private final int                                        diskSizeCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES
      .getValueAsInteger();
  private final List<WeakReference<OLowDiskSpaceListener>> listeners             = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();

  private final AtomicLong lastDiskSpaceCheck = new AtomicLong(0);
  private final String storagePath;

  private final ConcurrentSkipListMap<PageKey, OCachePointer>      writeCachePages     = new ConcurrentSkipListMap<PageKey, OCachePointer>();
  private final ConcurrentSkipListMap<OLogSequenceNumber, PageKey> pagesByLSN          = new ConcurrentSkipListMap<OLogSequenceNumber, PageKey>();
  private final ConcurrentSkipListSet<PageKey>                     exclusiveWritePages = new ConcurrentSkipListSet<PageKey>();

  private final ODistributedCounter writeCacheSize          = new ODistributedCounter();
  private final ODistributedCounter exclusiveWriteCacheSize = new ODistributedCounter();
  private final ODistributedCounter cacheOverflowCount      = new ODistributedCounter();

  private final OBinarySerializer<String>                    stringSerializer;
  private final OClosableLinkedContainer<Long, OFileClassic> files;

  private final boolean        syncOnPageFlush;
  private final int            pageSize;
  private final OWriteAheadLog writeAheadLog;
  private final AtomicLong amountOfNewPagesAdded = new AtomicLong();

  private final OPartitionedLockManager<PageKey> lockManager = new OPartitionedLockManager<PageKey>();
  private final OLocalPaginatedStorage storageLocal;
  private final OReadersWriterSpinLock filesLock = new OReadersWriterSpinLock();
  private final ScheduledExecutorService commitExecutor;

  private final ExecutorService lowSpaceEventsPublisher;
  private final AtomicBoolean checkpointInProgress = new AtomicBoolean();

  private volatile ConcurrentMap<String, Integer> nameIdMap;

  private       RandomAccessFile nameIdMapHolder;
  private final int              exclusiveWriteCacheMaxSize;
  private final int              writeCacheMaxSize;

  private int fileCounter = 1;

  private PageKey lastWritePageKey = new PageKey(0, -1);

  private File nameIdMapHolderFile;

  private final int id;

  private long chunkCount   = 0;
  private long chunkSizeSum = 0;

  private long flushedPagesSum = 0;
  private long flushCount      = 0;

  private long reportTime = 0;

  private PageKey lastFlushedKey = null;

  private final OPerformanceStatisticManager performanceStatisticManager;

  private final OByteBufferPool bufferPool;

  /**
   * Listeners which are called when exception in background data flush thread is happened.
   */
  private final List<WeakReference<OBackgroundExceptionListener>> backgroundExceptionListeners = new CopyOnWriteArrayList<WeakReference<OBackgroundExceptionListener>>();

  public OWOWCache(boolean syncOnPageFlush, int pageSize, OByteBufferPool bufferPool, OWriteAheadLog writeAheadLog,
      long pageFlushInterval, long exclusiveWriteCacheMaxSize, long writeCacheMaxSize, OLocalPaginatedStorage storageLocal,
      boolean checkMinSize, OClosableLinkedContainer<Long, OFileClassic> files, int id) {
    filesLock.acquireWriteLock();
    try {
      this.id = id;
      this.files = files;

      this.syncOnPageFlush = syncOnPageFlush;
      this.pageSize = pageSize;
      this.writeAheadLog = writeAheadLog;
      this.bufferPool = bufferPool;

      int writeNormalizedSize = normalizeMemory(exclusiveWriteCacheMaxSize, pageSize);
      if (checkMinSize && writeNormalizedSize < MIN_CACHE_SIZE)
        writeNormalizedSize = MIN_CACHE_SIZE;

      int normalizedSize = normalizeMemory(writeCacheMaxSize, pageSize);
      if (checkMinSize && normalizedSize < MIN_CACHE_SIZE)
        normalizedSize = MIN_CACHE_SIZE;

      this.exclusiveWriteCacheMaxSize = writeNormalizedSize;
      this.writeCacheMaxSize = normalizedSize;

      this.storageLocal = storageLocal;

      this.storagePath = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath());
      this.performanceStatisticManager = storageLocal.getPerformanceStatisticManager();

      final OBinarySerializerFactory binarySerializerFactory = storageLocal.getComponentsFactory().binarySerializerFactory;
      this.stringSerializer = binarySerializerFactory.getObjectSerializer(OType.STRING);

      commitExecutor = Executors.newSingleThreadScheduledExecutor(new FlushThreadFactory(storageLocal.getName()));
      lowSpaceEventsPublisher = Executors.newCachedThreadPool(new LowSpaceEventsPublisherFactory(storageLocal.getName()));

      if (pageFlushInterval > 0) {
        //We limit amount of pages per flush to allow to continue to read data from disk during massive flushes
        //so we divide maximum amount of pages flushed per second to amount of flushes per second.
        maxPagesPerFlush = (int) (OGlobalConfiguration.DISK_WRITE_CACHE_PAGES_PER_SECOND.getValueAsInteger() / (1000.0
            / pageFlushInterval));
        commitExecutor.scheduleWithFixedDelay(new PeriodicFlushTask(), pageFlushInterval, pageFlushInterval, TimeUnit.MILLISECONDS);
      }

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Loads files already registered in storage.
   * Has to be called before usage of this cache
   */
  public void loadRegisteredFiles() throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Adds listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to trigger
   */
  public void addBackgroundExceptionListener(OBackgroundExceptionListener listener) {
    backgroundExceptionListeners.add(new WeakReference<OBackgroundExceptionListener>(listener));
  }

  /**
   * Removes listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to remove
   */
  public void removeBackgroundExceptionListener(OBackgroundExceptionListener listener) {
    List<WeakReference<OBackgroundExceptionListener>> itemsToRemove = new ArrayList<WeakReference<OBackgroundExceptionListener>>();

    for (WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener l = ref.get();
      if (l != null && l.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    for (WeakReference<OBackgroundExceptionListener> ref : itemsToRemove) {
      backgroundExceptionListeners.remove(ref);
    }
  }

  /**
   * Fires event about exception is thrown in data flush thread
   */
  private void fireBackgroundDataProcessingExceptionEvent(Exception e) {
    for (WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener listener = ref.get();
      if (listener != null) {
        listener.onException(e);
      }
    }
  }

  private int normalizeMemory(final long maxSize, final int pageSize) {
    final long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  /**
   * Directory which contains all files managed by write cache.
   *
   * @return Directory which contains all files managed by write cache or <code>null</code> in case of in memory database.
   */
  @Override
  public File getRootDirectory() {
    return new File(storagePath);
  }

  @Override
  public OPerformanceStatisticManager getPerformanceStatisticManager() {
    return performanceStatisticManager;
  }

  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    listeners.add(new WeakReference<OLowDiskSpaceListener>(listener));
  }

  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    final List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<WeakReference<OLowDiskSpaceListener>>();

    for (WeakReference<OLowDiskSpaceListener> ref : listeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OLowDiskSpaceListener> ref : itemsToRemove)
      listeners.remove(ref);
  }

  private void freeSpaceCheckAfterNewPageAdd() {
    final long newPagesAdded = amountOfNewPagesAdded.incrementAndGet();
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (newPagesAdded - lastSpaceCheck > diskSizeCheckInterval || lastSpaceCheck == 0) {
      final File storageDir = new File(storagePath);

      final long freeSpace = storageDir.getFreeSpace();

      if (freeSpace < freeSpaceLimit)
        callLowSpaceListeners(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));

      lastDiskSpaceCheck.lazySet(newPagesAdded);
    }
  }

  private void callLowSpaceListeners(final OLowDiskSpaceInformation information) {
    lowSpaceEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (WeakReference<OLowDiskSpaceListener> lowDiskSpaceListenerWeakReference : listeners) {
          final OLowDiskSpaceListener listener = lowDiskSpaceListenerWeakReference.get();
          if (listener != null)
            try {
              listener.lowDiskSpace(information);
            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "Error during notification of low disk space for storage " + storageLocal.getName(), e);
            }
        }
      }
    });
  }

  private static int calculatePageCrc(byte[] pageData) {
    final int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  public long bookFileId(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      final Integer fileId = nameIdMap.get(fileName);

      if (fileId != null && fileId < 0) {
        return composeFileId(id, -fileId);
      }

      ++fileCounter;

      return composeFileId(id, fileCounter);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public int pageSize() {
    return pageSize;
  }

  @Override
  public boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  public long loadFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      //check that file is already registered
      if (!(fileId == null || fileId < 0)) {
        final long externalId = composeFileId(id, fileId);
        fileClassic = files.get(externalId);

        if (fileClassic != null)
          return externalId;
        else
          throw new OStorageException("File with given name " + fileName + " only partially registered in storage");
      }

      fileClassic = createFileInstance(fileName);
      if (!fileClassic.exists())
        throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageLocal.getName());
      else {
        // REGISTER THE FILE
        OLogManager.instance().debug(this,
            "File '" + fileName + "' is not registered in 'file name - id' map, but exists in file system. Registering it");

        if (fileId == null) {
          ++fileCounter;
          fileId = fileCounter;
        } else
          fileId = -fileId;

        openFile(fileClassic);

        final long externalId = composeFileId(id, fileId);
        files.add(externalId, fileClassic);

        nameIdMap.put(fileName, fileId);
        writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

        return externalId;
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long addFile(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      if (fileId != null && fileId >= 0)
        throw new OStorageException("File with name " + fileName + " already exists in storage " + storageLocal.getName());

      if (fileId == null) {
        ++fileCounter;
        fileId = fileCounter;
      } else
        fileId = -fileId;

      fileClassic = createFileInstance(fileName);
      createFile(fileClassic);

      final long externalId = composeFileId(id, fileId);
      files.add(externalId, fileClassic);

      nameIdMap.put(fileName, fileId);
      writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

      return externalId;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long fileIdByName(String fileName) {
    final Integer intId = nameIdMap.get(fileName);

    if (intId == null || intId < 0)
      return -1;

    return composeFileId(id, intId);
  }

  @Override
  public int internalFileId(long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(int fileId) {
    return composeFileId(id, fileId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLogSequenceNumber getMinimalNotFlushedLSN() {
    Map.Entry<OLogSequenceNumber, PageKey> entry = pagesByLSN.firstEntry();
    if (entry == null)
      return null;

    return entry.getKey();
  }

  public long addFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      OFileClassic fileClassic;

      Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId)
          throw new OStorageException(
              "File with name '" + fileName + "'' already exists in storage '" + storageLocal.getName() + "'");
        else
          throw new OStorageException(
              "File with given name already exists but has different id " + existingFileId + " vs. proposed " + fileId);
      }

      fileId = composeFileId(id, intId);
      fileClassic = files.get(fileId);

      if (fileClassic != null) {
        if (!fileClassic.getName().equals(fileName))
          throw new OStorageException(
              "File with given id exists but has different name " + fileClassic.getName() + " vs. proposed " + fileName);
      } else {
        if (fileCounter < intId)
          fileCounter = intId;

        fileClassic = createFileInstance(fileName);
        createFile(fileClassic);

        files.add(fileId, fileClassic);
      }

      nameIdMap.put(fileName, intId);
      writeNameIdEntry(new NameFileIdEntry(fileName, intId), true);

      return fileId;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean checkLowDiskSpace() {
    final File storageDir = new File(storagePath);

    final long freeSpace = storageDir.getFreeSpace();
    return freeSpace < freeSpaceLimit;
  }

  @Override
  public void flushTillSegment(long segmentId) {
    Future<Void> future = commitExecutor.submit(new FlushTillSegmentTask(segmentId));
    try {
      future.get();
    } catch (Exception e) {
      throw ODatabaseException.wrapException(new OStorageException("Error during data flush"), e);
    }

  }

  public void makeFuzzyCheckpoint(long segmentId) throws IOException {
    if (writeAheadLog != null) {
      filesLock.acquireReadLock();
      try {
        OLogSequenceNumber startLSN = writeAheadLog.begin(segmentId);
        if (startLSN == null)
          return;

        writeAheadLog.logFuzzyCheckPointStart(startLSN);

        for (Integer intId : nameIdMap.values()) {
          if (intId < 0)
            continue;

          final long fileId = composeFileId(id, intId);
          final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
          try {
            final OFileClassic fileClassic = entry.get();
            fileClassic.synch();
          } finally {
            files.release(entry);
          }
        }

        writeAheadLog.logFuzzyCheckPointEnd();
        writeAheadLog.flush();

        writeAheadLog.cutAllSegmentsSmallerThan(segmentId);
      } finally {
        filesLock.releaseReadLock();
      }
    }
  }

  public boolean exists(String fileName) {
    filesLock.acquireReadLock();
    try {
      if (nameIdMap != null) {
        Integer fileId = nameIdMap.get(fileName);

        if (fileId != null && fileId >= 0)
          return true;
      }

      final File file = new File(
          storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath() + File.separator + fileName));
      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public boolean exists(long fileId) {
    filesLock.acquireReadLock();
    try {
      final int intId = extractFileId(fileId);
      fileId = composeFileId(id, intId);

      final OFileClassic file = files.get(fileId);

      if (file == null)
        return false;

      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public Future store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    Future future = null;

    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      final PageKey pageKey = new PageKey(intId, pageIndex);
      Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
      try {
        final OCachePointer pagePointer = writeCachePages.get(pageKey);

        if (pagePointer == null) {
          writeCachePages.put(pageKey, dataPointer);

          final OLogSequenceNumber firstChangedLSN = dataPointer.getFirstChangedLSN();

          if (firstChangedLSN != null) {
            assert !pagesByLSN.containsKey(firstChangedLSN);

            pagesByLSN.put(firstChangedLSN, pageKey);
          }

          writeCacheSize.increment();

          dataPointer.setWritersListener(this);
          dataPointer.incrementWritersReferrer();
        } else
          assert pagePointer.equals(dataPointer);

      } finally {
        lockManager.releaseLock(groupLock);
      }

      if (exclusiveWriteCacheSize.get() > exclusiveWriteCacheMaxSize) {
        cacheOverflowCount.increment();
        future = commitExecutor.submit(new PeriodicFlushTask());
      }

      return future;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public Map<String, Long> files() {
    filesLock.acquireReadLock();
    try {
      final Map<String, Long> result = new HashMap<String, Long>();

      for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getValue()));
        }
      }

      return result;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public OCachePointer[] load(long fileId, long startPageIndex, int pageCount, boolean addNewPages, OModifiableBoolean cacheHit)
      throws IOException {
    final int intId = extractFileId(fileId);
    if (pageCount < 1)
      throw new IllegalArgumentException("Amount of pages to load should be not less than 1 but provided value is " + pageCount);

    filesLock.acquireReadLock();
    try {
      final PageKey[] pageKeys = new PageKey[pageCount];
      for (int i = 0; i < pageCount; i++) {
        pageKeys[i] = new PageKey(intId, startPageIndex + i);
      }

      Lock[] pageLocks = lockManager.acquireSharedLocksInBatch(pageKeys);
      try {
        OCachePointer pagePointer = writeCachePages.get(pageKeys[0]);

        if (pagePointer == null) {
          final OCachePointer pagePointers[] = cacheFileContent(intId, startPageIndex, pageCount, addNewPages, cacheHit);

          if (pagePointers.length == 0)
            return pagePointers;

          for (int n = 0; n < pagePointers.length; n++) {
            pagePointers[n].incrementReadersReferrer();

            if (n > 0) {
              pagePointer = writeCachePages.get(pageKeys[n]);

              assert pageKeys[n].pageIndex == pagePointers[n].getPageIndex();

              if (pagePointer != null) {
                pagePointers[n].decrementReadersReferrer();
                pagePointers[n] = pagePointer;
              }
            }
          }

          return pagePointers;
        }

        pagePointer.incrementReadersReferrer();

        cacheHit.setValue(true);

        return new OCachePointer[] { pagePointer };
      } finally {
        for (Lock lock : pageLocks) {
          lock.unlock();
        }
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void addOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.increment();
    exclusiveWritePages.add(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void removeOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.decrement();
    exclusiveWritePages.remove(new PageKey(extractFileId(fileId), pageIndex));
  }

  public void flush(final long fileId) {
    final Future<Void> future = commitExecutor.submit(new FileFlushTask(extractFileId(fileId)));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OInterruptedException("File flush was interrupted");
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  public void flush() {
    for (int intId : nameIdMap.values()) {
      if (intId < 0)
        continue;

      final long externalId = composeFileId(id, intId);
      flush(externalId);
    }

  }

  public long getFilledUpTo(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        return entry.get().getFileSize() / pageSize;
      } finally {
        files.release(entry);
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public long getExclusiveWriteCachePagesSize() {
    return exclusiveWriteCacheSize.get();
  }

  public void deleteFile(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      final String name = doDeleteFile(intId);

      if (name != null) {
        nameIdMap.put(name, -intId);
        writeNameIdEntry(new NameFileIdEntry(name, -intId), true);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void truncateFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      removeCachedPages(intId);
      OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        entry.get().shrink(0);
      } finally {
        files.release(entry);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      if (entry == null)
        return;

      try {
        OFileClassic file = entry.get();
        final String osFileName = file.getName();
        if (osFileName.startsWith(oldFileName)) {
          final File newFile = new File(storageLocal.getStoragePath() + File.separator + newFileName + osFileName
              .substring(osFileName.lastIndexOf(oldFileName) + oldFileName.length()));
          boolean renamed = file.renameTo(newFile);
          while (!renamed) {
            renamed = file.renameTo(newFile);
          }
        }
      } finally {
        files.release(entry);
      }

      nameIdMap.remove(oldFileName);
      nameIdMap.put(newFileName, intId);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, intId), true);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long[] close() throws IOException {
    flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OWriteCacheException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw OException.wrapException(new OWriteCacheException("Data flush thread was interrupted"), e);
      }
    }

    final List<Long> result = new ArrayList<Long>();

    filesLock.acquireWriteLock();
    try {
      final Collection<Integer> intIds = nameIdMap.values();

      for (Integer intId : intIds) {
        if (intId < 0)
          continue;

        final long fileId = composeFileId(id, intId);
        //we remove files because when we reopen storage we will reload them
        final OFileClassic fileClassic = files.remove(fileId);
        fileClassic.close();

        result.add(fileId);
      }

      if (nameIdMapHolder != null) {
        nameIdMapHolder.setLength(0);

        for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
          writeNameIdEntry(new NameFileIdEntry(entry.getKey(), entry.getValue()), false);
        }
        nameIdMapHolder.getFD().sync();
        nameIdMapHolder.close();
      }

      nameIdMap.clear();

      final long[] ds = new long[result.size()];
      int counter = 0;
      for (long id : result) {
        ds[counter] = id;
        counter++;
      }

      return ds;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void close(long fileId, boolean flush) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      if (flush)
        flush(intId);
      else
        removeCachedPages(intId);

      if (!files.close(fileId))
        throw new OStorageException("Can not close file with id " + internalFileId(fileId) + " because it is still in use");
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;
    final List<OPageDataVerificationError> errors = new ArrayList<OPageDataVerificationError>();

    filesLock.acquireWriteLock();
    try {
      for (Integer intId : nameIdMap.values()) {
        if (intId < 0)
          continue;

        boolean fileIsCorrect;
        final long externalId = composeFileId(id, intId);
        final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
        final OFileClassic fileClassic = entry.get();
        try {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Flashing file " + fileClassic.getName() + "... ");

          flush(intId);

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Start verification of content of " + fileClassic.getName() + "file ...");

          long time = System.currentTimeMillis();

          long filledUpTo = fileClassic.getFileSize();
          fileIsCorrect = true;

          for (long pos = 0; pos < filledUpTo; pos += pageSize) {
            boolean checkSumIncorrect = false;
            boolean magicNumberIncorrect = false;

            byte[] data = new byte[pageSize];

            fileClassic.read(pos, data, data.length);

            long magicNumber = OLongSerializer.INSTANCE.deserializeNative(data, 0);

            if (magicNumber != MAGIC_NUMBER) {
              magicNumberIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage(
                    "Error: Magic number for page " + (pos / pageSize) + " in file " + fileClassic.getName()
                        + " does not much !!!");
              fileIsCorrect = false;
            }

            final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, OLongSerializer.LONG_SIZE);

            final int calculatedCRC32 = calculatePageCrc(data);
            if (storedCRC32 != calculatedCRC32) {
              checkSumIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage(
                    "Error: Checksum for page " + (pos / pageSize) + " in file " + fileClassic.getName() + " is incorrect !!!");
              fileIsCorrect = false;
            }

            if (magicNumberIncorrect || checkSumIncorrect)
              errors.add(
                  new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileClassic.getName()));

            if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
              time = notificationTimeOut;
              commandOutputListener.onMessage((pos / pageSize) + " pages were processed ...");
            }
          }
        } catch (IOException ioe) {
          if (commandOutputListener != null)
            commandOutputListener
                .onMessage("Error: Error during processing of file " + fileClassic.getName() + ". " + ioe.getMessage());

          fileIsCorrect = false;
        } finally {
          files.release(entry);
        }

        if (!fileIsCorrect) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is finished with errors.");
        } else {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is successfully finished.");
        }
      }

      return errors.toArray(new OPageDataVerificationError[errors.size()]);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long[] delete() throws IOException {
    final List<Long> result = new ArrayList<Long>();

    filesLock.acquireWriteLock();
    try {
      for (int intId : nameIdMap.values()) {
        if (intId < 0)
          continue;

        final long externalId = composeFileId(id, intId);
        doDeleteFile(externalId);
        result.add(externalId);
      }

      if (nameIdMapHolderFile != null) {
        if (nameIdMapHolderFile.exists()) {
          nameIdMapHolder.close();

          if (!nameIdMapHolderFile.delete())
            throw new OStorageException("Cannot delete disk cache file which contains name-id mapping.");
        }

        nameIdMapHolder = null;
        nameIdMapHolderFile = null;
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OWriteCacheException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OInterruptedException("Data flush thread was interrupted");
      }
    }

    final long[] ids = new long[result.size()];
    int counter = 0;
    for (long id : result) {
      ids[counter] = id;
      counter++;
    }

    return ids;
  }

  public String fileNameById(long fileId) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {

      final OFileClassic f = files.get(fileId);
      return f != null ? f.getName() : null;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public int getId() {
    return id;
  }

  public long getCacheOverflowCount() {
    return cacheOverflowCount.get();
  }

  public long getWriteCacheSize() {
    return writeCacheSize.get();
  }

  public long getExclusiveWriteCacheSize() {
    return exclusiveWriteCacheSize.get();
  }

  private void openFile(final OFileClassic fileClassic) throws IOException {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen())
        fileClassic.open();
    } else {
      throw new OStorageException("File " + fileClassic + " does not exist.");
    }

  }

  private void createFile(final OFileClassic fileClassic) throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create();
      fileClassic.synch();
    } else {
      throw new OStorageException("File '" + fileClassic.getName() + "' already exists.");
    }
  }

  private void initNameIdMapping() throws IOException {
    if (nameIdMapHolder == null) {
      final File storagePath = new File(storageLocal.getStoragePath());
      if (!storagePath.exists())
        if (!storagePath.mkdirs())
          throw new OStorageException("Cannot create directories for the path '" + storagePath + "'");

      nameIdMapHolderFile = new File(storagePath, NAME_ID_MAP);

      nameIdMapHolder = new RandomAccessFile(nameIdMapHolderFile, "rw");
      readNameIdMap();
    }
  }

  private OFileClassic createFileInstance(String fileName) {
    final String path = storageLocal.getVariableParser()
        .resolveVariables(storageLocal.getStoragePath() + File.separator + fileName);
    return new OFileClassic(path, storageLocal.getMode());
  }

  private void readNameIdMap() throws IOException {
    nameIdMap = new ConcurrentHashMap<String, Integer>();
    long localFileCounter = -1;

    nameIdMapHolder.seek(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntry()) != null) {

      final long absFileId = Math.abs(nameFileIdEntry.fileId);
      if (localFileCounter < absFileId)
        localFileCounter = absFileId;

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
    }

    if (localFileCounter > 0)
      fileCounter = (int) localFileCounter;

    for (Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          OFileClassic fileClassic = createFileInstance(nameIdEntry.getKey());

          if (fileClassic.exists()) {
            fileClassic.open();
            files.add(externalId, fileClassic);
          } else {
            final Integer fileId = nameIdMap.get(nameIdEntry.getKey());

            if (fileId != null && fileId > 0) {
              nameIdMap.put(nameIdEntry.getKey(), -fileId);
            }
          }
        }
      }
    }
  }

  private NameFileIdEntry readNextNameIdEntry() throws IOException {
    try {
      final int nameSize = nameIdMapHolder.readInt();
      byte[] serializedName = new byte[nameSize];

      nameIdMapHolder.readFully(serializedName);

      final String name = stringSerializer.deserialize(serializedName, 0);
      final int fileId = (int) nameIdMapHolder.readLong();

      return new NameFileIdEntry(name, fileId);
    } catch (EOFException eof) {
      return null;
    }
  }

  private void writeNameIdEntry(NameFileIdEntry nameFileIdEntry, boolean sync) throws IOException {

    nameIdMapHolder.seek(nameIdMapHolder.length());

    final int nameSize = stringSerializer.getObjectSize(nameFileIdEntry.name);
    byte[] serializedRecord = new byte[OIntegerSerializer.INT_SIZE + nameSize + OLongSerializer.LONG_SIZE];
    OIntegerSerializer.INSTANCE.serializeLiteral(nameSize, serializedRecord, 0);
    stringSerializer.serialize(nameFileIdEntry.name, serializedRecord, OIntegerSerializer.INT_SIZE);
    OLongSerializer.INSTANCE.serializeLiteral(nameFileIdEntry.fileId, serializedRecord, OIntegerSerializer.INT_SIZE + nameSize);

    nameIdMapHolder.write(serializedRecord);

    if (sync)
      nameIdMapHolder.getFD().sync();
  }

  private String doDeleteFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    removeCachedPages(intId);

    final OFileClassic fileClassic = files.remove(fileId);

    String name = null;
    if (fileClassic != null) {
      name = fileClassic.getName();

      if (fileClassic.exists())
        fileClassic.delete();
    }

    return name;
  }

  private void removeCachedPages(int fileId) {
    //cache already closed or deleted
    if (commitExecutor.isShutdown())
      return;

    Future<Void> future = commitExecutor.submit(new RemoveFilePagesTask(fileId));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OInterruptedException("File data removal was interrupted");
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File data removal was abnormally terminated"), e);
    }
  }

  private OCachePointer[] cacheFileContent(final int intId, final long startPageIndex, final int pageCount,
      final boolean addNewPages, OModifiableBoolean cacheHit) throws IOException {

    final long fileId = composeFileId(id, intId);
    final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
    try {
      final OFileClassic fileClassic = entry.get();
      if (fileClassic == null)
        throw new IllegalArgumentException("File with id " + intId + " not found in WOW Cache");

      final long firstPageStartPosition = startPageIndex * pageSize;
      final long firstPageEndPosition = firstPageStartPosition + pageSize;

      if (fileClassic.getFileSize() >= firstPageEndPosition) {
        final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
            .getSessionPerformanceStatistic();
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
        }

        int pagesRead = 0;

        try {
          if (pageCount == 1) {
            final ByteBuffer buffer = bufferPool.acquireDirect(false);
            fileClassic.read(firstPageStartPosition, buffer);
            buffer.position(0);

            final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, fileId, startPageIndex);
            pagesRead = 1;
            return new OCachePointer[] { dataPointer };
          }

          final long maxPageCount = (fileClassic.getFileSize() - firstPageStartPosition) / pageSize;
          final int realPageCount = Math.min((int) maxPageCount, pageCount);

          final ByteBuffer[] buffers = new ByteBuffer[realPageCount];
          for (int i = 0; i < buffers.length; i++) {
            buffers[i] = bufferPool.acquireDirect(false);
            assert buffers[i].position() == 0;
          }

          final long bytesRead = fileClassic.read(firstPageStartPosition, buffers);
          assert bytesRead % pageSize == 0;

          final int buffersRead = (int) (bytesRead / pageSize);

          final OCachePointer[] dataPointers = new OCachePointer[buffersRead];
          for (int n = 0; n < buffersRead; n++) {
            buffers[n].position(0);
            dataPointers[n] = new OCachePointer(buffers[n], bufferPool, fileId, startPageIndex + n);
          }

          for (int n = buffersRead; n < buffers.length; n++) {
            bufferPool.release(buffers[n]);
          }

          pagesRead = dataPointers.length;
          return dataPointers;
        } finally {
          if (sessionStoragePerformanceStatistic != null) {
            sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(pagesRead);
          }

        }
      } else if (addNewPages) {
        final int space = (int) (firstPageEndPosition - fileClassic.getFileSize());

        if (space > 0)
          fileClassic.allocateSpace(space);

        freeSpaceCheckAfterNewPageAdd();

        final ByteBuffer buffer = bufferPool.acquireDirect(true);
        final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, fileId, startPageIndex);

        cacheHit.setValue(true);
        return new OCachePointer[] { dataPointer };
      } else
        return new OCachePointer[0];
    } finally {
      files.release(entry);
    }
  }

  private void flushPage(final int fileId, final long pageIndex, final ByteBuffer buffer) throws IOException {
    if (writeAheadLog != null) {
      final OLogSequenceNumber lsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
      final OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = new byte[pageSize];
    buffer.position(0);
    buffer.get(content);

    OLongSerializer.INSTANCE.serializeNative(MAGIC_NUMBER, content, 0);

    final int crc32 = calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final long externalId = composeFileId(id, fileId);
    final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
    try {
      final OFileClassic fileClassic = entry.get();
      fileClassic.write(pageIndex * pageSize, content);

      if (syncOnPageFlush)
        fileClassic.synch();
    } finally {
      files.release(entry);
    }
  }

  private void preparePageToFlush(final ByteBuffer buffer) throws IOException {
    if (writeAheadLog != null) {
      final OLogSequenceNumber lsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
      final OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = new byte[pageSize];
    buffer.position(0);
    buffer.get(content);

    final int crc32 = calculatePageCrc(content);

    buffer.position(0);
    buffer.putLong(MAGIC_NUMBER);
    buffer.putInt(crc32);
  }

  private static final class NameFileIdEntry {
    private final String name;
    private final int    fileId;

    private NameFileIdEntry(String name, int fileId) {
      this.name = name;
      this.fileId = fileId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      NameFileIdEntry that = (NameFileIdEntry) o;

      if (fileId != that.fileId)
        return false;
      if (!name.equals(that.name))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + fileId;
      return result;
    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final int  fileId;
    private final long pageIndex;

    private PageKey(final int fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(final PageKey other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (pageIndex > other.pageIndex)
        return 1;
      if (pageIndex < other.pageIndex)
        return -1;

      return 0;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId)
        return false;
      if (pageIndex != pageKey.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fileId;
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    public PageKey previous() {
      return pageIndex == -1 ? this : new PageKey(fileId, pageIndex - 1);
    }
  }

  private final class FlushTillSegmentTask implements Callable<Void> {
    private final long segmentId;

    public FlushTillSegmentTask(long segmentId) {
      this.segmentId = segmentId;
    }

    @Override
    public Void call() throws Exception {
      OLogSequenceNumber minDirtyLSN = getEarliestDirtyLSN();
      Map.Entry<OLogSequenceNumber, PageKey> firstEntry;

      while (minDirtyLSN != null && minDirtyLSN.getSegment() < segmentId) {
        flushWriteCacheFromMinLSN(1);

        firstEntry = pagesByLSN.firstEntry();
        if (firstEntry != null)
          minDirtyLSN = firstEntry.getKey();
        else
          minDirtyLSN = null;
      }

      return null;
    }

    private OLogSequenceNumber getEarliestDirtyLSN() {
      Map.Entry<OLogSequenceNumber, PageKey> firstEntry = pagesByLSN.firstEntry();
      OLogSequenceNumber minDirtyLSN;
      if (firstEntry != null)
        minDirtyLSN = firstEntry.getKey();
      else
        minDirtyLSN = null;
      return minDirtyLSN;
    }
  }

  private final class PeriodicFlushTask implements Runnable {
    @Override
    public void run() {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      if (statistic != null)
        statistic.startWriteCacheFlushTimer();

      final long ms = System.currentTimeMillis();

      if (reportTime == 0)
        reportTime = ms;
      else if (ms - reportTime > 10000) {
        reportTime = ms;

        System.out.println("Average chunk size : " + (chunkSizeSum / chunkCount));
        System.out.println("Average flush in pages : " + (flushedPagesSum / flushCount));

        chunkSizeSum = 0;
        chunkCount = 0;

        flushedPagesSum = 0;
        flushCount = 0;
      }

      int flushedPages = 0;
      try {
        if (writeCachePages.isEmpty()) {
          return;
        }

        // cache is split on two types of buffers
        //
        // 1) buffer which contains pages are shared with read buffer
        // 2) pages which are exclusively held by write cache
        //
        // last type of buffer usually small and if it is close to overflow we should flush it first
        // this last type of buffer contains pages are sorted by position of pages in file and logically forms ring buffer,
        // so it will be faster to flush it if needed
        final long ewcs = exclusiveWriteCacheSize.get();
        assert ewcs >= 0;
        final double exclusiveWriteCacheThreshold = ((double) ewcs) / exclusiveWriteCacheMaxSize;

        if (exclusiveWriteCacheThreshold > 0.3) {
          int exclusiveWritePagesToFlush = (int) Math.floor(((exclusiveWriteCacheThreshold - 0.3) / 0.4) * maxPagesPerFlush);
          exclusiveWritePagesToFlush = Math.max(4, Math.min(maxPagesPerFlush, exclusiveWritePagesToFlush));

          flushedPages = flushExclusiveWritePagesRing(exclusiveWritePagesToFlush, flushedPages);
        }

        final long wcs = writeCacheSize.get();
        assert wcs >= 0;

        final double cacheThreshold = ((double) wcs) / writeCacheMaxSize;
        int writePagesToFlush = 0;
        if (cacheThreshold > 0.3) {
          writePagesToFlush = (int) Math.floor(((cacheThreshold - 0.3) / 0.4) * maxPagesPerFlush);
        }

        writePagesToFlush = Math.max(4, Math.min(maxPagesPerFlush, writePagesToFlush));

        flushWriteCacheFromMinLSN(writePagesToFlush);

      } catch (IOException e) {
        OLogManager.instance().error(this, "Exception during data flush", e);
        OWOWCache.this.fireBackgroundDataProcessingExceptionEvent(e);
      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "Exception during data flush", e);
        OWOWCache.this.fireBackgroundDataProcessingExceptionEvent(e);
      } finally {
        if (statistic != null)
          statistic.stopWriteCacheFlushTimer(flushedPages);
      }
    }
  }

  private void flushWriteCacheFromMinLSN(int amountOfPagesToFlush) throws IOException {
    //flag whether we iterate over the second part of the ring
    boolean nextRound = false;

    //amount of pages flushed at the moment
    int flushedPages = 0;

    //iterator which will be used to iterate over ring buffer
    Iterator<Map.Entry<PageKey, OCachePointer>> pageIterator;

    //first we try to find page which contains the oldest not flushed changes
    //that is needed to allow to compact WAL as earlier as possible
    final Map.Entry<OLogSequenceNumber, PageKey> firstMinLSNEntry = pagesByLSN.firstEntry();

    //key in round buffer from which iteration was started
    PageKey initialKey = null;

    if (firstMinLSNEntry != null) {
      final PageKey minPageKey = firstMinLSNEntry.getValue();
      pageIterator = writeCachePages.tailMap(minPageKey).entrySet().iterator();
    } else {
      if (lastFlushedKey == null) {
        pageIterator = writeCachePages.entrySet().iterator();
      } else {
        pageIterator = writeCachePages.tailMap(lastFlushedKey, false).entrySet().iterator();
      }
    }

    //we go through virtual index which starts from 0 and ends with pages.size()-1
    //but inside of cycle will use real index which starts from flushIndex and ends with
    // flushIndex -1
    Map.Entry<PageKey, OCachePointer> lastEntry = null;

    while (flushedPages < amountOfPagesToFlush) {
      List<OCachePointer> chunk = new ArrayList<OCachePointer>();
      PageKey lastPageKey = null;
      PageKey chunkPageKey = null;

      if (lastEntry != null) {
        chunk.add(lastEntry.getValue());
        chunkPageKey = lastEntry.getKey();
        lastPageKey = chunkPageKey;

        lastEntry = null;
      }

      if (!pageIterator.hasNext()) {
        if (nextRound)
          break;

        if (initialKey != null) {
          pageIterator = writeCachePages.headMap(initialKey, false).entrySet().iterator();
          nextRound = true;
        } else {
          break;
        }
      }

      while (chunk.size() < 10 && pageIterator.hasNext()) {
        final Map.Entry<PageKey, OCachePointer> entry = pageIterator.next();
        final PageKey pageKey = entry.getKey();
        final OCachePointer page = entry.getValue();

        if (initialKey == null)
          initialKey = pageKey;

        if (chunk.isEmpty()) {
          chunk.add(page);

          chunkPageKey = entry.getKey();
        } else {
          if (chunkPageKey.fileId == pageKey.fileId && lastPageKey.pageIndex + 1 == entry.getKey().pageIndex) {
            chunk.add(entry.getValue());
          } else {
            lastEntry = entry;
            break;
          }
        }

        lastPageKey = entry.getKey();
      }

      if (!chunk.isEmpty()) {
        chunkCount++;
        chunkSizeSum += chunk.size();

        boolean flushed = false;

        if (chunk.size() > 1) {
          final PageKey[] pageKeys = new PageKey[chunk.size()];

          for (int n = 0; n < pageKeys.length; n++) {
            final OCachePointer cachePointer = chunk.get(n);
            pageKeys[n] = new PageKey(internalFileId(cachePointer.getFileId()), cachePointer.getPageIndex());
          }

          final Lock[] locks = lockManager.acquireExclusiveLocksInBatch(pageKeys);
          try {
            boolean acquired = true;

            for (int n = 0; n < chunk.size(); n++) {
              final OCachePointer cachePointer = chunk.get(n);

              if (!cachePointer.tryAcquireSharedLock()) {
                for (int k = 0; k < n; k++) {
                  final OCachePointer pointer = chunk.get(k);
                  pointer.releaseSharedLock();
                }

                acquired = false;
                break;
              }
            }

            if (acquired) {
              final List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(chunk.size());

              for (final OCachePointer cachePointer : chunk) {
                final ByteBuffer buffer = cachePointer.getSharedBuffer();
                buffers.add(buffer);

                preparePageToFlush(buffer);
                buffer.position(0);
              }

              final OClosableEntry<Long, OFileClassic> entry = files.acquire(composeFileId(id, chunkPageKey.fileId));
              try {
                final OFileClassic fileClassic = entry.get();
                fileClassic.write(chunkPageKey.pageIndex * pageSize, buffers.toArray(new ByteBuffer[0]));
              } finally {
                files.release(entry);
              }

              for (OCachePointer pointer : chunk) {
                final OLogSequenceNumber firstChangedLSN = pointer.getFirstChangedLSN();

                pointer.clearFirstChangedLSN();

                pointer.decrementWritersReferrer();
                pointer.setWritersListener(null);

                final PageKey pageKey = new PageKey(internalFileId(pointer.getFileId()), pointer.getPageIndex());
                writeCachePages.remove(pageKey);

                lastFlushedKey = pageKey;

                if (firstChangedLSN != null)
                  pagesByLSN.remove(firstChangedLSN);

                writeCacheSize.decrement();

                pointer.releaseSharedLock();
              }

              flushed = true;

              flushedPages += chunk.size();
            }
          } finally {
            for (Lock lock : locks)
              lock.unlock();
          }
        }

        if (!flushed) {
          for (OCachePointer cachePointer : chunk) {
            final PageKey pageKey = new PageKey(internalFileId(cachePointer.getFileId()), cachePointer.getPageIndex());
            final Lock lock = lockManager.acquireExclusiveLock(pageKey);
            try {
              if (!cachePointer.tryAcquireSharedLock())
                continue;

              try {
                flushPage(pageKey.fileId, pageKey.pageIndex, cachePointer.getSharedBuffer());

                final OLogSequenceNumber firstChangedLSN = cachePointer.getFirstChangedLSN();

                cachePointer.clearFirstChangedLSN();
                cachePointer.decrementWritersReferrer();

                cachePointer.setWritersListener(null);

                writeCachePages.remove(pageKey);
                if (firstChangedLSN != null)
                  pagesByLSN.remove(firstChangedLSN);

                writeCacheSize.decrement();

                flushedPages++;
              } finally {
                cachePointer.releaseSharedLock();
              }

              lastFlushedKey = pageKey;

            } finally {
              lock.unlock();
            }
          }
        }
      }
    }

    flushCount++;
    flushedPagesSum += flushedPages;
  }

  private int flushExclusiveWritePagesRing(final int writePagesToFlush, int flushedPages) throws IOException {
    final NavigableSet<PageKey> writePagesSubset = exclusiveWritePages.tailSet(lastWritePageKey, false);
    flushedPages = iterateByWritePagesSubRing(writePagesSubset, writePagesToFlush, flushedPages);

    if (flushedPages < writePagesToFlush) {
      flushedPages = iterateByWritePagesSubRing(exclusiveWritePages, writePagesToFlush, flushedPages);
    }

    return flushedPages;
  }

  private int iterateByWritePagesSubRing(final NavigableSet<PageKey> subSet, final int writePagesToFlush, int flushedWritePages)
      throws IOException {
    final Iterator<PageKey> entriesIterator = subSet.iterator();
    int flushedRegions = 0;

    long lastPageIndex = -1;
    while (entriesIterator.hasNext()) {
      PageKey entry = entriesIterator.next();
      if (lastPageIndex >= 0) {
        if (entry.pageIndex != lastPageIndex + 1) {
          flushedRegions++;
        }
      }

      if (flushedWritePages > writePagesToFlush && flushedRegions >= 4)
        break;

      Lock groupLock = lockManager.acquireExclusiveLock(entry);
      try {
        OCachePointer pagePointer = writeCachePages.get(entry);

        if (pagePointer == null) {
          entriesIterator.remove();
          continue;
        }

        if (!pagePointer.tryAcquireSharedLock())
          continue;

        final OLogSequenceNumber firstChangedLSN;
        try {
          final ByteBuffer buffer = pagePointer.getSharedBuffer();
          flushPage(entry.fileId, entry.pageIndex, buffer);

          firstChangedLSN = pagePointer.getFirstChangedLSN();
          pagePointer.clearFirstChangedLSN();
        } finally {
          pagePointer.releaseSharedLock();
        }

        pagePointer.decrementWritersReferrer();
        pagePointer.setWritersListener(null);

        entriesIterator.remove();
        writeCachePages.remove(entry);

        if (firstChangedLSN != null)
          pagesByLSN.remove(firstChangedLSN);

      } finally {
        lockManager.releaseLock(groupLock);
      }

      lastWritePageKey = entry;
      flushedWritePages++;

      lastPageIndex = entry.pageIndex;

      writeCacheSize.decrement();
    }

    return flushedWritePages;
  }

  private final class FileFlushTask implements Callable<Void> {
    private final int fileId;

    private FileFlushTask(final int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final PageKey firstKey = new PageKey(fileId, 0);
      final PageKey lastKey = new PageKey(fileId, Long.MAX_VALUE);

      final Iterator<Map.Entry<PageKey, OCachePointer>> entryIterator = writeCachePages.subMap(firstKey, true, lastKey, true)
          .entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<PageKey, OCachePointer> entry = entryIterator.next();
        final PageKey pageKey = entry.getKey();
        final OCachePointer pagePointer = entry.getValue();

        final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
        try {
          if (!pagePointer.tryAcquireSharedLock())
            continue;

          OLogSequenceNumber firstChangedLSN;
          try {
            final ByteBuffer buffer = pagePointer.getSharedBuffer();
            flushPage(pageKey.fileId, pageKey.pageIndex, buffer);
            firstChangedLSN = pagePointer.getFirstChangedLSN();

            pagePointer.clearFirstChangedLSN();
          } finally {
            pagePointer.releaseSharedLock();
          }

          if (firstChangedLSN != null)
            pagesByLSN.remove(firstChangedLSN);

          pagePointer.decrementWritersReferrer();
          pagePointer.setWritersListener(null);

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(groupLock);
        }

        writeCacheSize.decrement();
      }

      final long finalId = composeFileId(id, fileId);
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(finalId);
      try {
        entry.get().synch();
      } finally {
        files.release(entry);
      }

      return null;
    }
  }

  private final class RemoveFilePagesTask implements Callable<Void> {
    private final int fileId;

    private RemoveFilePagesTask(int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final PageKey firstKey = new PageKey(fileId, 0);
      final PageKey lastKey = new PageKey(fileId, Long.MAX_VALUE);

      Iterator<Map.Entry<PageKey, OCachePointer>> entryIterator = writeCachePages.subMap(firstKey, true, lastKey, true).entrySet()
          .iterator();
      while (entryIterator.hasNext()) {
        Map.Entry<PageKey, OCachePointer> entry = entryIterator.next();

        final OCachePointer pagePointer = entry.getValue();
        final PageKey pageKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
        try {
          pagePointer.acquireExclusiveLock();
          try {
            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);
            writeCacheSize.decrement();
          } finally {
            pagePointer.releaseExclusiveLock();
          }

          OLogSequenceNumber firstChangedLSN = pagePointer.getFirstChangedLSN();

          if (firstChangedLSN != null)
            pagesByLSN.remove(firstChangedLSN);

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }

      return null;
    }
  }

  private static class FlushThreadFactory implements ThreadFactory {
    private final String storageName;

    private FlushThreadFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setPriority(Thread.MAX_PRIORITY);
      thread.setName("OrientDB Write Cache Flush Task (" + storageName + ")");
      return thread;
    }
  }

  private static class LowSpaceEventsPublisherFactory implements ThreadFactory {
    private final String storageName;

    private LowSpaceEventsPublisherFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB Low Disk Space Publisher (" + storageName + ")");
      return thread;
    }
  }
}
