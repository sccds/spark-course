package com.twq.util;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    // 将每一条交易记录解析成item列表
    public static List<String> toList(String transaction, String delimiter) {
        String[] items = transaction.trim().split(delimiter);
        List<String> list = new ArrayList<String>();
        for (String item : items) {
            list.add(item);
        }
        return list;
    }

    public static String list2string(List<String> list) {
        StringBuilder stringBuilder = new StringBuilder();
        int listSize = list.size();
        for (int i = 0; i < listSize; i++) {
            stringBuilder.append(list.get(i));
            if (i != listSize - 1) {
                stringBuilder.append(",");
            }
        }
        return stringBuilder.toString();
    }
}
