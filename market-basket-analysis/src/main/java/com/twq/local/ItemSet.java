package com.twq.local;

import java.util.HashSet;
import java.util.Set;

public class ItemSet {

    // 该项目集合抱恨的项目（即商品）
    private Set<String> items = new HashSet<String>();

    // 该项目集合的在所有交易中出现的次数
    private int supportCount;

    public ItemSet() {}

    public ItemSet(Set<String> items) { this.items = items; }

    public Set<String> getItems() { return items; }

    public void setItems(Set<String> items) { this.items = items; }

    public int setSupportCount() { return supportCount; }

    public void setSupportCount(int supportCount) { this.supportCount = supportCount; }



}
