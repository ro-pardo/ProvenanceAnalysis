package org.thesis;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LineageNode {
    private Object discarded;
    private List<Map<String, String>> passed;
    private List<Integer> parents;
    private List<String> keys;

    LineageNode() {
    }

    public Object getDiscarded() {
        return discarded;
    }

    public List<Map<String, String>> getPassed() {
        return passed;
    }

    public List<Integer> getParents() {
        return parents;
    }

    public List<String> getKeys() {
        return keys;
    }
}
