/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.substrait.rel;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.ReadRel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class EncodedLocalFiles implements SplitInfo {

  public static final class Builder {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final CodedOutputStream output = CodedOutputStream.newInstance(bytes);

    public void addFile(ReadRel.LocalFiles.FileOrFiles file) {
      try {
        output.writeMessage(ReadRel.LocalFiles.ITEMS_FIELD_NUMBER, file);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public EncodedLocalFiles build(List<String> locations) {
      try {
        output.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return new EncodedLocalFiles(bytes.toByteArray(), locations);
    }
  }

  private final byte[] bytes;
  private final List<String> locations;

  public EncodedLocalFiles(byte[] bytes, List<String> locations) {
    this.bytes = Objects.requireNonNull(bytes, "bytes");
    this.locations = Collections.unmodifiableList(new ArrayList<>(locations));
  }

  @Override
  public List<String> preferredLocations() {
    return locations;
  }

  @Override
  public byte[] toProtobufBytes() {
    return bytes;
  }

  @Override
  public ReadRel.LocalFiles toProtobuf() {
    try {
      return ReadRel.LocalFiles.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Invalid encoded local files", e);
    }
  }
}
