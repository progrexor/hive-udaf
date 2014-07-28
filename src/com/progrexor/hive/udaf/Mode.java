package com.progrexor.hive.udaf;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@SuppressWarnings("deprecation")
public final class Mode extends UDAF {
  public static class Evaluator implements UDAFEvaluator {
    private HashMap<String, Long> buffer;

    public Evaluator() {
      init();
    }

    public void init() {
      buffer = new HashMap<String, Long>();
    }

    public boolean iterate(String key) {
      if (!buffer.containsKey(key)) {
        buffer.put(key, 1L);
      } else {
        Long val = buffer.get(key);
        buffer.put(key, val + 1);
      }
      return true;
    }

    public HashMap<String, Long> terminatePartial() {
      return buffer;
    }

    public boolean merge(HashMap<String, Long> another) {
      if (another == null) {
        return true;
      }
      for (String key : another.keySet()) {
        if (!buffer.containsKey(key)) {
          buffer.put(key, another.get(key));
        } else {
          buffer.put(key, buffer.get(key) + another.get(key));
        }
      }
      return true;
    }

    public String terminate() {
      if (buffer.size() == 0) {
        return null;
      }
      return findMax(buffer);
    }

    public String findMax(HashMap<String, Long> buffer) {
      Entry<String, Long> maxEntry = null;
      for (Entry<String, Long> entry : buffer.entrySet()) {
        if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
          maxEntry = entry;
        }
      }
      return maxEntry.getKey();
    }
  }
}
