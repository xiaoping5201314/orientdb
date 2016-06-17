package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.index.lsmtree.cuckoofilter.OCuckooArray;

public class OCuckooArrayException extends ODurableComponentException {
  public OCuckooArrayException(OCuckooArrayException exception) {
    super(exception);
  }

  public OCuckooArrayException(String message, OCuckooArray array) {
    super(message, array);
  }
}
