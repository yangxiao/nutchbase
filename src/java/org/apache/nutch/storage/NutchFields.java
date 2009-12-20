package org.apache.nutch.storage;

public interface NutchFields {
  public static final String STATUS = "status";
  public static final String FETCH_TIME = "fetchTime";
  public static final String FETCH_INTERVAL = "fetchInterval";
  public static final String RETRIES = "retriesSinceFetch";
  public static final String SCORE = "score";
  public static final String TITLE = "title";
  public static final String TEXT = "text";
  public static final String OUTLINKS = "outlinks";
  public static final String METADATA = "metadata";

  public static final String[] ALL_FIELDS = new String[] {
    STATUS, FETCH_TIME, FETCH_INTERVAL, RETRIES, SCORE,
    TITLE, TEXT, OUTLINKS, METADATA
  };
}
