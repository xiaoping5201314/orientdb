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

package com.orientechnologies.orient.core.db.record.ridbag;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

public interface ORidBagDelegate
    extends Iterable<OIdentifiable>, ORecordLazyMultiValue, OTrackedMultiValue<OIdentifiable, OIdentifiable> {
  void addAll(Collection<OIdentifiable> values);

  void add(OIdentifiable identifiable);

  void remove(OIdentifiable identifiable);

  boolean isEmpty();

  int getSerializedSize(ORidBag.Encoding encoding);

  int getSerializedSize(byte[] stream, int offset, ORidBag.Encoding encoding);

  /**
   * Writes content of bag to stream.
   * 
   * OwnerUuid is needed to notify db about changes of collection pointer if some happens during serialization.
   * @param bytes TODO
   * @param ownerUuid id of delegate owner
   * 
   * @return offset where content of stream is ended
   */
  int serialize(BytesContainer bytes, UUID ownerUuid, ORidBag.Encoding encoding);

  int deserialize(BytesContainer bytes, ORidBag.Encoding encoding);

  void requestDelete();

  /**
   * THIS IS VERY EXPENSIVE METHOD AND CAN NOT BE CALLED IN REMOTE STORAGE.
   *
   * @param identifiable Object to check.
   * @return true if ridbag contains at leas one instance with the same rid as passed in identifiable.
   */
  boolean contains(OIdentifiable identifiable);

  public void setOwner(ORecord owner);

  public ORecord getOwner();

  public String toString();

  public List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> getChangeListeners();


}
