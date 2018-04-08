/*
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
package org.apache.beam.sdk.transforms.splittabledofn;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link RestrictionTracker} for claiming {@link ByteKey}s in a {@link ByteKeyRange} in a
 * monotonically increasing fashion.
 */
public class ByteKeyRangeTracker extends RestrictionTracker<ByteKeyRange, ByteKey> {
  private ByteKeyRange range;
  @Nullable private ByteKey lastClaimedKey = null;
  @Nullable private ByteKey lastAttemptedKey = null;

  private ByteKeyRangeTracker(ByteKeyRange range) {
    this.range = checkNotNull(range);
  }

  /**
   * Instantiates a new {@link ByteKeyRangeTracker} with the specified range. The keys in the range
   * are left padded to be the same length in bytes.
   */
  public static ByteKeyRangeTracker of(ByteKeyRange range) {
    return new ByteKeyRangeTracker(ByteKeyRange.of(range.getStartKey(), range.getEndKey()));
  }

  @Override
  public synchronized ByteKeyRange currentRestriction() {
    return range;
  }

  @Override
  public synchronized ByteKeyRange checkpoint() {
    checkState(lastClaimedKey != null, "Can't checkpoint before any successful claim");
    final ByteKey nextKey = next(lastClaimedKey, range);
    // hack to force range to be bigger than the range upper bundle because ByteKeyRange *start*
    // must always be less than *end*.
    ByteKey rangeEndKey =
        (nextKey.equals(range.getEndKey())) ? next(range.getEndKey(), range) : range.getEndKey();
    ByteKeyRange res = ByteKeyRange.of(nextKey, rangeEndKey);
    this.range = ByteKeyRange.of(range.getStartKey(), nextKey);
    return res;
  }

  /**
   * Attempts to claim the given key.
   *
   * <p>Must be larger than the last successfully claimed key.
   *
   * @return {@code true} if the key was successfully claimed, {@code false} if it is outside the
   *     current {@link ByteKeyRange} of this tracker (in that case this operation is a no-op).
   */
  @Override
  protected synchronized boolean tryClaimImpl(ByteKey key) {
    checkArgument(
        lastAttemptedKey == null || key.compareTo(lastAttemptedKey) > 0,
        "Trying to claim key %s while last attempted was %s",
        key,
        lastAttemptedKey);
    checkArgument(
        key.compareTo(range.getStartKey()) > -1,
        "Trying to claim key %s before start of the range %s",
        key,
        range);
    lastAttemptedKey = key;
    // No respective checkArgument for i < range.to() - it's ok to try claiming keys beyond
    if (!range.getEndKey().isEmpty() && key.compareTo(range.getEndKey()) > -1) {
      return false;
    }
    lastClaimedKey = key;
    return true;
  }

  /**
   * Marks that there are no more keys to be claimed in the range.
   *
   * <p>E.g., a {@link DoFn} reading a file and claiming the key of each record in the file might
   * call this if it hits EOF - even though the last attempted claim was before the end of the
   * range, there are no more keys to claim.
   */
  public synchronized void markDone() {
    lastAttemptedKey = range.getEndKey();
  }

  @Override
  public synchronized void checkDone() throws IllegalStateException {
    checkState(
        lastAttemptedKey.compareTo(range.getEndKey()) >= 0
            || next(lastAttemptedKey, range).equals(range.getEndKey()),
        "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastAttemptedKey,
        range,
        next(lastAttemptedKey, range),
        range.getEndKey());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("range", range)
        .add("lastClaimedKey", lastClaimedKey)
        .add("lastAttemptedKey", lastAttemptedKey)
        .toString();
  }

  // Utility methods
  @VisibleForTesting
  static ByteKey next(ByteKey key, @Nullable ByteKeyRange range) {
    // An empty ByteKey may represent a lower or upper bound, we use the range to infer it.
    if (key.isEmpty() && range != null) {
      if (range.getStartKey().isEmpty()) {
        return ByteKey.of(0);
      } else if (range.getEndKey().isEmpty()) {
        return ByteKey.EMPTY;
      }
    }
    return ByteKey.copyFrom(unsignedCopyAndIncrement(key.getBytes()));
  }

  // Following methods cloned from org.apache.hadoop.hbase.util.Bytes
  private static byte[] unsignedCopyAndIncrement(byte[] input) {
    byte[] copy = copy(input);
    if (copy == null) {
      throw new IllegalArgumentException("cannot increment null array");
    } else {
      for (int i = copy.length - 1; i >= 0; --i) {
        if (copy[i] != -1) {
          ++copy[i];
          return copy;
        }
        copy[i] = 0;
      }
      byte[] out = new byte[copy.length + 1];
      out[0] = 1;
      System.arraycopy(copy, 0, out, 1, copy.length);
      return out;
    }
  }

  private static byte[] copy(byte[] bytes) {
    byte[] result = new byte[bytes.length];
    System.arraycopy(bytes, 0, result, 0, bytes.length);
    return result;
  }
}
