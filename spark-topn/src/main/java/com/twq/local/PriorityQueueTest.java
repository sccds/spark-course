package com.twq.local;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * java堆结构 PriorityQueue 完全解析:  https://www.cnblogs.com/tstd/p/5125949.html
 */
public class PriorityQueueTest {
    public static void main(String[] args) {
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>(10, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        // 添加元素 offer
        priorityQueue.offer(100L);
        priorityQueue.offer(23L);
        priorityQueue.offer(44L);
        priorityQueue.offer(55L);
        priorityQueue.offer(12L);
        priorityQueue.offer(66L);
        priorityQueue.offer(3L);
        priorityQueue.offer(5L);
        priorityQueue.offer(1L);

        // 第一个元素一定是最小的
        System.out.println(priorityQueue);

        // peek 拿出来的是第一个元素，最小的, 数据不变
        Long head = priorityQueue.peek();
        System.out.println(head);
        System.out.println(priorityQueue);

        // poll 拿出来的是第一个元素，最小的, 数据删除，队列改变
        Long pollHead = priorityQueue.poll();
        System.out.println(pollHead);
        System.out.println(priorityQueue);
    }
}
