package com.twq.topn;

import scala.Serializable;
import scala.Tuple2;
;
import java.util.Comparator;

public class WordCountComparator implements Comparator<Tuple2<String, Long>>, Serializable {
    @Override
    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
        return o2._2.compareTo(o1._2);
    }
}
