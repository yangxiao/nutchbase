package org.apache.nutch.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class NutchHashMap<K, V> extends HashMap<K, V> {
  
  public static enum State {
    NOT_UPDATED, UPDATED, DELETED
  }
  
  /* This is probably a terrible design but I do not yet have a better
   * idea of managing write/delete info on a per-key basis
   */
  private Map<K, State> keyStates = new HashMap<K, State>();

  public NutchHashMap() {
    this(null);
  }

  public NutchHashMap(Map<K, V> m) {
    super();
    if (m == null) {
      return;
    }
    super.putAll(m);
  }
  
  @Override
  public V put(K key, V value) {
    keyStates.put(key, State.UPDATED);
    return super.put(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    if (keyStates.containsKey(key)) {
      keyStates.put((K) key, State.DELETED);
    }
    return super.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Entry<? extends K, ? extends V> e : m.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public void clear() {
    for (Entry<K, V> e : entrySet()) {
      keyStates.put(e.getKey(), State.DELETED);
    }
    super.clear();
  }

  public void resetStates() {
    keyStates.clear();
  }

  public void putState(K key, State state) {
    keyStates.put(key, state);
  }

  public Map<K, State> states() {
    return keyStates;
  }
}
