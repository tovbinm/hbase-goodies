/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.base.Preconditions;

/**
 * A filter, based on the ColumnCountGetFilter, takes two arguments: limit and offset, or limit and column offset.
 * This filter can be used for row-based indexing, where references to other tables are stored across many columns,
 * in order to efficient lookups and paginated results for end users. Only most recent versions are considered
 * for pagination.
 */
public class ColumnPaginationByteOffsetFilter extends FilterBase
{
  private int limit = 0;
  private int offset = -1;
  private byte[] columnOffset = null;
  private int count = 0;

  /**
   * Used during serialization. Do not use.
   */
  public ColumnPaginationByteOffsetFilter()
  {
    super();
  }
  /**
   * Initializes filter with an integer offset and limit. The offset is arrived at
   * scanning sequentially and skipping entries. @limit number of columns are
   * then retrieved. If multiple column families are involved, the columns may be spread
   * across them.
   *
   * @param limit Max number of columns to return.
   * @param offset The integer offset where to start pagination.
   */
  public ColumnPaginationByteOffsetFilter(final int limit, final int offset)
  {
    Preconditions.checkArgument(limit >= 0, "limit must be positive %s", limit);
    Preconditions.checkArgument(offset >= 0, "offset must be positive %s", offset);
    this.limit = limit;
    this.offset = offset;
  }
  /**
   * Initialized filter with a string/bookmark based offset and limit. The offset is arrived
   * at, by seeking to it using scanner hints. If multiple column families are involved,
   * pagination starts at the first column family which contains @columnOffset. Columns are
   * then retrieved sequentially upto @limit number of columns which maybe spread across
   * multiple column families, depending on how the scan is setup.
   *
   * @param limit Max number of columns to return.
   * @param columnOffset The string/bookmark offset on where to start pagination.
   */
  public ColumnPaginationByteOffsetFilter(final int limit, final byte[] columnOffset) {
    Preconditions.checkArgument(limit >= 0, "limit must be positive %s", limit);
    Preconditions.checkArgument(columnOffset != null,
                                "columnOffset must be non-null %s",
                                columnOffset);
    this.limit = limit;
    this.columnOffset = columnOffset;
  }

  /**
   * @return limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return columnOffset
   */
  public byte[] getColumnOffset() {
    return columnOffset;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv)
  {
    if (columnOffset != null) {
      if (count >= limit) {
        return ReturnCode.NEXT_ROW;
      }
      byte[] buffer = kv.getBuffer();
      if (buffer == null) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
      int cmp = 0;
      // Only compare if no KV's have been seen so far.
      if (count == 0) {
        cmp = Bytes.compareTo(buffer,
                              kv.getQualifierOffset(),
                              kv.getQualifierLength(),
                              this.columnOffset,
                              0,
                              this.columnOffset.length);
      }
      if (cmp < 0) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      } else {
        count++;
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
    } else {
      if (count >= offset + limit) {
        return ReturnCode.NEXT_ROW;
      }

      ReturnCode code = count < offset ? ReturnCode.NEXT_COL :
                                         ReturnCode.INCLUDE_AND_NEXT_COL;
      count++;
      return code;
    }
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    byte[] buffer = kv.getBuffer();
    return KeyValue.createFirstOnRow(
        buffer, kv.getRowOffset(), kv.getRowLength(), buffer,
        kv.getFamilyOffset(), kv.getFamilyLength(), columnOffset, 0, columnOffset.length);
  }

  @Override
  public void reset()
  {
    this.count = 0;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 2,
                                "Expected 2 but got: %s", filterArguments.size());
    int limit = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
    int offset = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
    return new ColumnPaginationFilter(limit, offset);
  }

  public void readFields(DataInput in) throws IOException
  {
    this.limit = in.readInt();
    this.offset = in.readInt();
    this.columnOffset = Bytes.readByteArray(in);
    if (this.columnOffset.length == 0) {
      this.columnOffset = null;
    }
  }

  public void write(DataOutput out) throws IOException
  {
    out.writeInt(this.limit);
    out.writeInt(this.offset);
    Bytes.writeByteArray(out, this.columnOffset);
  }

  @Override
  public String toString() {
    if (this.columnOffset != null) {
      return (this.getClass().getSimpleName() + "(" + this.limit + ", " +
          Bytes.toStringBinary(this.columnOffset) + ")");
    }
    return String.format("%s (%d, %d)", this.getClass().getSimpleName(),
        this.limit, this.offset);
  }
}
