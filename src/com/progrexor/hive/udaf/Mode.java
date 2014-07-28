package com.progrexor.hive.udaf;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@SuppressWarnings("deprecation")
public final class Mode extends UDAF {
  public static class Evaluator implements UDAFEvaluator {
    private HashMap<String, Integer> buffer;

    public Evaluator() {
      init();
    }

    public void init() {
      buffer = new HashMap<String, Integer>();
    }

    public boolean iterate(String key) {
      if (!buffer.containsKey(key)) {
        buffer.put(key, 1);
      } else {
        Integer val = buffer.get(key);
        buffer.put(key, val + 1);
      }
      return true;
    }

    public HashMap<String, Integer> terminatePartial() {
      return buffer;
    }

    public boolean merge(HashMap<String, Integer> another) {
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

    public String findMax(HashMap<String, Integer> buffer) {
      Entry<String, Integer> maxEntry = null;
      for (Entry<String, Integer> entry : buffer.entrySet()) {
        if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
          maxEntry = entry;
        }
      }
      return maxEntry.getKey();
    }
  }
}
