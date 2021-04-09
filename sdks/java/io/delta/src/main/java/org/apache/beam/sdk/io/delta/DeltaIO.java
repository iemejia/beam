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
package org.apache.beam.sdk.io.delta;

import com.google.auto.value.AutoValue;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileIO.Match;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO
})
/** TODO A Beam IO */
public class DeltaIO {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaIO.class);

  public static Match match() {
    return new AutoValue_DeltaIO_Match.Builder().build();
  }

  /** Implementation of {@link #match}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<Metadata>> {
    abstract @Nullable ValueProvider<String> getPath();

    abstract @Nullable ValueProvider<Long> getVersion();

    abstract @Nullable ValueProvider<Long> getVersionTimestamp();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setPath(ValueProvider<String> path);

      abstract Builder setVersion(ValueProvider<Long> path);

      abstract Builder setVersionTimestamp(ValueProvider<Long> path);

      abstract Match build();
    }

    /** Matches the given filepattern. */
    public Match path(String path) {
      return path(StaticValueProvider.of(path));
    }

    /** Like {@link #path(String)} but using a {@link ValueProvider}. */
    public Match path(ValueProvider<String> path) {
      return toBuilder().setPath(path).build();
    }

    public Match version(long version) {
      return version(StaticValueProvider.of(version));
    }

    public Match version(ValueProvider<Long> version) {
      return toBuilder().setVersion(version).build();
    }

    public Match versionTimestamp(long versionTimestamp) {
      return versionTimestamp(StaticValueProvider.of(versionTimestamp));
    }

    public Match versionTimestamp(ValueProvider<Long> versionTimestamp) {
      return toBuilder().setVersionTimestamp(versionTimestamp).build();
    }

    @Override
    public PCollection<Metadata> expand(PBegin input) {
      DeltaLog log = DeltaLog.forTable(new Configuration(), getPath().get());
      final Snapshot snapshot;
      if (getVersion() != null) {
        snapshot = log.getSnapshotForVersionAsOf(getVersion().get());
      } else if (getVersionTimestamp() != null) {
        snapshot = log.getSnapshotForTimestampAsOf(getVersionTimestamp().get());
      } else {
        snapshot = log.snapshot();
      }

      List<String> deltaFilesPaths =
          snapshot.getAllFiles().stream()
              .map(addFile -> Paths.get(getPath().get(), addFile.getPath()).toString())
              .collect(Collectors.toList());
      LOG.debug("Delta files: " + deltaFilesPaths.toString());

      List<Metadata> metadata = new ArrayList<>();
      try {
        List<MatchResult> match = FileSystems.match(deltaFilesPaths, EmptyMatchTreatment.DISALLOW);
        for (MatchResult matchResult : match) {
          metadata.addAll(matchResult.metadata());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return input.apply("Matched Delta files", Create.of(metadata));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("path", getPath()).withLabel("Delta Table Path"));
      builder.addIfNotNull(DisplayData.item("version", getVersion()).withLabel("Version"));
      Instant versionTimeStamp = Instant.ofEpochMilli(getVersionTimestamp().get());
      builder.addIfNotNull(
          DisplayData.item("versionTimeStamp", versionTimeStamp.toString())
              .withLabel("Version Timestamp"));
    }
  }
}
