/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.sdk;

import com.typesafe.config.*;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class loads configuration from a Typesafe Config and provides type-safe getters with
 * one-time logging of property values.
 */
public class DASSettings {

  private static final Logger logger = LoggerFactory.getLogger(DASSettings.class);

  // Synchronization lock and a set to track which property keys have been logged.
  private static final Object ALREADY_LOGGED_LOCK = new Object();
  private static final Set<String> ALREADY_LOGGED_KEYS = new HashSet<>();

  private final Config config;

  /** Exception representing a settings configuration problem. */
  public static class SettingsException extends RuntimeException {
    public SettingsException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Constructs DASSettings with a default configuration (from the classpath). */
  public DASSettings() {
    this(ConfigFactory.load());
  }

  /**
   * Constructs DASSettings with a provided Config instance.
   *
   * @param config the Config instance to use
   */
  public DASSettings(Config config) {
    this.config = Objects.requireNonNull(config, "config must not be null");
  }

  /**
   * Renders the entire configuration as a single-line JSON string.
   *
   * @return The configuration as a string.
   * @throws SettingsException if there's an issue rendering the configuration.
   */
  public String renderAsString() throws SettingsException {
    try {
      return config.root().render(ConfigRenderOptions.concise());
    } catch (ConfigException ex) {
      throw new SettingsException("Error writing configuration: " + ex.getMessage(), ex);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, ConfigValue> entry : config.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue().unwrapped();
      sb.append(key).append(" -> ").append(value).append("; ");
    }
    // Remove trailing "; " if present
    if (sb.length() > 2) {
      sb.setLength(sb.length() - 2);
    }
    return sb.toString();
  }

  /**
   * Retrieves a string property.
   *
   * @param property The property name.
   * @param logValue Whether to log its value.
   * @return The string value.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public String getString(String property, boolean logValue) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          String value = config.getString(property);
          logOneTime(property, logValue ? value : "*****", "string");
          return value;
        });
  }

  public String getString(String property) throws SettingsException {
    return getString(property, true);
  }

  /**
   * Retrieves an optional string property.
   *
   * @param property The property name.
   * @param logValue Whether to log its value.
   * @return An Optional containing the string value if found, else empty.
   * @throws SettingsException if there's an issue other than a missing property.
   */
  public Optional<String> getStringOpt(String property, boolean logValue) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          try {
            String value = config.getString(property);
            logOneTime(property, logValue ? value : "*****", "string");
            return Optional.of(value);
          } catch (ConfigException.Missing e) {
            return Optional.empty();
          }
        });
  }

  public Optional<String> getStringOpt(String property) throws SettingsException {
    return getStringOpt(property, true);
  }

  /**
   * Retrieves a boolean property.
   *
   * @param property The property name.
   * @return The boolean value.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public boolean getBoolean(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          boolean value = config.getBoolean(property);
          logOneTime(property, value, "boolean");
          return value;
        });
  }

  /**
   * Retrieves an optional boolean property.
   *
   * @param property The property name.
   * @return An Optional containing the boolean value if found, else empty.
   * @throws SettingsException if there's an issue other than missing property.
   */
  public Optional<Boolean> getBooleanOpt(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          try {
            boolean value = config.getBoolean(property);
            logOneTime(property, value, "boolean");
            return Optional.of(value);
          } catch (ConfigException.Missing e) {
            return Optional.empty();
          }
        });
  }

  /**
   * Retrieves an integer property.
   *
   * @param property The property name.
   * @return The integer value.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public int getInt(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          int value = config.getInt(property);
          logOneTime(property, value, "int");
          return value;
        });
  }

  /**
   * Retrieves an optional integer property.
   *
   * @param property The property name.
   * @return An Optional containing the integer value if found, else empty.
   * @throws SettingsException if there's an issue other than missing property.
   */
  public Optional<Integer> getIntOpt(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          try {
            int value = config.getInt(property);
            logOneTime(property, value, "int");
            return Optional.of(value);
          } catch (ConfigException.Missing e) {
            return Optional.empty();
          }
        });
  }

  /**
   * Retrieves a Duration property.
   *
   * @param property The property name.
   * @return The Duration value.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public Duration getDuration(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          Duration value = config.getDuration(property);
          logOneTime(property, value, "duration");
          return value;
        });
  }

  /**
   * Retrieves an optional Duration property.
   *
   * @param property The property name.
   * @return An Optional containing the Duration value if found, else empty.
   * @throws SettingsException if there's an issue other than missing property.
   */
  public Optional<Duration> getDurationOpt(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          try {
            Duration value = config.getDuration(property);
            logOneTime(property, value, "duration");
            return Optional.of(value);
          } catch (ConfigException.Missing e) {
            return Optional.empty();
          }
        });
  }

  /**
   * Retrieves a value as a size in bytes (parses special strings like "128M").
   *
   * @param property The property name.
   * @return The long value.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public Long getBytes(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          Long value = config.getBytes(property);
          logOneTime(property, value, "bytes");
          return value;
        });
  }

  /**
   * Retrieves an optional value as a size in bytes (parses special strings like "128M").
   *
   * @param property
   * @return An Optional containing the long value if found, else empty.
   * @throws SettingsException if there's an issue other than missing property.
   */
  public Optional<Long> getBytesOpt(String property) throws SettingsException {
    return withLogConfigException(
        property,
        () -> {
          try {
            Long value = config.getBytes(property);
            logOneTime(property, value, "bytes");
            return Optional.of(value);
          } catch (ConfigException.Missing e) {
            return Optional.empty();
          }
        });
  }

  /**
   * Retrieves a subtree of the configuration as a map of strings.
   *
   * @param property The property name.
   * @return The subtree as a map of strings.
   * @throws SettingsException if there's an issue retrieving the property.
   */
  public Optional<Set<Entry<String, ConfigValue>>> getConfigSubTree(String property) {
    try {
      return Optional.of(config.getConfig(property).root().entrySet());
    } catch (ConfigException.Missing e) {
      return Optional.empty();
    }
  }

  /** Runs a config access block and wraps any ConfigException in a SettingsException. */
  private <T> T withLogConfigException(String propertyName, ConfigSupplier<T> supplier)
      throws SettingsException {
    try {
      return supplier.get();
    } catch (ConfigException ex) {
      throw new SettingsException("Error loading property: " + propertyName, ex);
    }
  }

  /** Logs a property value once per property key. */
  private void logOneTime(String key, Object value, String propertyType) {
    synchronized (ALREADY_LOGGED_LOCK) {
      // We only log once per property key, not per value
      if (ALREADY_LOGGED_KEYS.add(key)) {
        logger.info("Using {}: {} ({})", key, value, propertyType);
      }
    }
  }

  @FunctionalInterface
  private interface ConfigSupplier<T> {
    T get() throws ConfigException;
  }
}
