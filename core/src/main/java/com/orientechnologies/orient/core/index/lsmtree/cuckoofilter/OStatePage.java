package com.orientechnologies.orient.core.index.lsmtree.cuckoofilter;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.io.IOException;

public class OStatePage extends ODurablePage {
  private static final int CAPACITY_OFFSET = NEXT_FREE_POSITION;

  public OStatePage(OCacheEntry cacheEntry, OWALChanges changes) {
    super(cacheEntry, changes);
  }

  public void setCapacity(int capacity) throws IOException {
    setIntValue(CAPACITY_OFFSET, capacity);
  }

  public int getCapacity() throws IOException {
    return getIntValue(CAPACITY_OFFSET);
  }
}
