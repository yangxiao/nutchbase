package org.apache.nutch.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class NutchHashMap<K, V> extends HashMap<K, V> {

  private static final long serialVersionUID = 4631273931677167192L;
  
  public static enum State {
    NOT_UPDATED, UPDATED, DELETED
  }
  
  /* This is probably a terrible design but I do not yet have a better
   * idea of managing write/delete info on a per-key basis
   */
  private Map<K, State> keyStates = new HashMap<K, State>();
  
  public NutchHashMap(Map<K, V> m) {
    super();
    super.putAll(m);
    for (K key : m.keySet()) {
      keyStates.put(key, State.NOT_UPDATED);
    }
  }
  
  @Override
  public V put(K key, V value) {
    if (keyStates.containsKey(key)) {
      keyStates.put(key, State.UPDATED);
    }
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

  public Iterator<Entry<K, State>> states() {
    return keyStates.entrySet().iterator();
  }
}
