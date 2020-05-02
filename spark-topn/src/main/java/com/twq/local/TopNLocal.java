package com.twq.local;

import java.util.Comparator;

public class TopNLocal {
    public static void main(String[] args) {
        int n = 3;
        BoundedPriorityQueue<Long> priorityQueue = new BoundedPriorityQueue<>(n, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });

        priorityQueue.add(100L);
        priorityQueue.add(23L);
        priorityQueue.add(44L);
        priorityQueue.add(55L);

        System.out.println(priorityQueue);

        priorityQueue.add(200L);
        System.out.println(priorityQueue);

        priorityQueue.add(308L);
        System.out.println(priorityQueue);

        priorityQueue.add(8L);
        System.out.println(priorityQueue);
    }
}
