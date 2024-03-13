/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import org.apache.hudi.common.util.Option;
import java.util.Properties;
import java.io.IOException;

public class CustomHoodieRecordPayload extends OverwriteNonDefaultsWithLatestAvroPayload {

  public CustomHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public CustomHoodieRecordPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }

    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);

    if (!isDeleteRecord(incomingRecord) && !needUpdatingPersistedRecord((GenericRecord) currentValue, incomingRecord)) {
      return Option.of(currentValue);
    }

    return super.combineAndGetUpdateValue(currentValue, schema, properties);
  }

  protected void setField(
      GenericRecord baseRecord,
      GenericRecord mergedRecord,
      GenericRecordBuilder builder,
      Schema.Field field) {
    Object value = baseRecord.get(field.name());
    value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
    Object defaultValue = field.defaultVal();
    String fieldName = field.name();
    if (!overwriteField(value, defaultValue) && fieldName != "unsuccessful_since") {
      builder.set(field, value);
    } else {
      builder.set(field, mergedRecord.get(fieldName));
    }
  }

  protected boolean needUpdatingPersistedRecord(GenericRecord currentValue, GenericRecord incomingRecord) {
    return !overwriteField(incomingRecord.get("error_type"), currentValue.get("error_type"));
  }

}