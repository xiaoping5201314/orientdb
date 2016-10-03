/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.util.Date;
import java.util.Random;

/**
 * @author Sergey Sitnikov
 */
public class HugeDatabaseTest {

  @Test
  public void generate() {
    // use `mvn clean -Dtest=HugeDatabaseTest#generate test -Ptest-embedded` to create a new database

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/HugeDatabaseTest");
    db.create();

    try {
      final Random random = new Random(555);
      generateClass(db, random, 0, 20, 30, 2, 5, 13000000);
      generateClass(db, random, 1, 20, 30, 2, 5, 313000000);
      generateClass(db, random, 2, 20, 30, 2, 5, 10000000);
    } finally {
      db.close();
    }
  }

  private void generateClass(ODatabaseDocumentTx db, Random random, int id, int minStringFields, int maxStringFields,
      int minDateFields, int maxDateFields, int records) {
    final String className = "class" + id;
    final OClass class_ = db.getMetadata().getSchema().createClass(className);

    final int stringFields = minStringFields + random.nextInt(maxStringFields - minStringFields);
    for (int i = 0; i < stringFields; ++i) {
      final OProperty property = class_.createProperty("string" + i, OType.STRING);
      switch (i % 5) {
      case 0: // unique
        property.createIndex(OClass.INDEX_TYPE.UNIQUE);
        break;
      case 1: // non-unique
        property.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
        break;
      default: // no index
        break;
      }
    }

    final int dateFields = minDateFields + random.nextInt(maxDateFields - minDateFields);
    for (int i = 0; i < dateFields; ++i) {
      final OProperty property = class_.createProperty("date" + i, OType.DATETIME);
      switch (i % 5) {
      case 0: // unique
        property.createIndex(OClass.INDEX_TYPE.UNIQUE);
        break;
      case 1: // non-unique
        property.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
        break;
      default: // no index
        break;
      }
    }

    final int[] uniqueStringCounters = new int[stringFields];
    final int[] uniqueDateCounters = new int[dateFields];

    for (int i = 0; i < records; ++i) {
      if (i % 1000 == 0)
        System.out.println(String.format("generated %d of %d, %.2f%%", i, records, i / (double) records * 100));

      final ODocument document = db.newInstance(className);

      for (int j = 0; j < stringFields; ++j)
        document.field("string" + j, randomString(random, 4, 24, j % 5 == 0 ? Integer.toString(uniqueStringCounters[j]++) : ""))
            .save();

      for (int j = 0; j < dateFields; ++j)
        document.field("date" + j, new Date((j % 5 == 0 ? uniqueDateCounters[j]++ : random.nextInt()) * 1000L)).save();
    }
  }

  private String randomString(Random random, int minLength, int maxLength, String postfix) {
    final int length = minLength + random.nextInt(maxLength - minLength);
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; ++i)
      builder.append((char) ('a' + random.nextInt('z' - 'a')));
    builder.append(postfix);
    return builder.toString();
  }

}
