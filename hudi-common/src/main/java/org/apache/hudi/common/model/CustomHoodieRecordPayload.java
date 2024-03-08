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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.util.Option;

import java.util.Properties;

public class CustomHoodieRecordPayload extends DefaultHoodieRecordPayload {

  public CustomHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public CustomHoodieRecordPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                IndexedRecord incomingRecord, Properties properties) {
    boolean incomingIsNewer = super.needUpdatingPersistedRecord(currentValue, incomingRecord, properties);
    GenericRecord current = (GenericRecord) currentValue;
    GenericRecord incoming = (GenericRecord) incomingRecord;
    boolean errorTypeUnchanged = overwriteField(incoming.get("error_type"), current.get("error_type"));
    return incomingIsNewer && !errorTypeUnchanged;
  }

}