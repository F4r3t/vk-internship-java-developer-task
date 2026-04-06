package ru.F4r3t.kv.model;

import java.util.List;

public record RangePage(List<KvEntry> items, String nextAfter) {
}