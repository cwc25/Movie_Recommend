package com.jacky.Util;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static List<String> toList(String record, String delimiter) {
        String[] items = record.trim().split(delimiter);
        List<String> list = new ArrayList<String>();
        for (String item : items) {
            list.add(item);
        }
        return list;
    }
}
