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
package org.apache.iceberg.spark.source;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

// Change the functions from private to public.
public class LogMessage {
  private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

  public static LogMessage debug(String date, String message) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "DEBUG", message);
  }

  public static LogMessage debug(String date, String message, Instant timestamp) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "DEBUG", message, timestamp);
  }

  public static LogMessage info(String date, String message) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "INFO", message);
  }

  public static LogMessage info(String date, String message, Instant timestamp) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "INFO", message, timestamp);
  }

  public static LogMessage error(String date, String message) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "ERROR", message);
  }

  public static LogMessage error(String date, String message, Instant timestamp) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "ERROR", message, timestamp);
  }

  public static LogMessage warn(String date, String message) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "WARN", message);
  }

  public static LogMessage warn(String date, String message, Instant timestamp) {
    return new LogMessage(ID_COUNTER.getAndIncrement(), date, "WARN", message, timestamp);
  }

  private int id;
  private String date;
  private String level;
  private String message;
  private Instant timestamp;

  private LogMessage(int id, String date, String level, String message) {
    this.id = id;
    this.date = date;
    this.level = level;
    this.message = message;
  }

  private LogMessage(int id, String date, String level, String message, Instant timestamp) {
    this.id = id;
    this.date = date;
    this.level = level;
    this.message = message;
    this.timestamp = timestamp;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }
}
