package com.hadoop.learn.com.hadoop.learn.ch5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Ly on 2018/6/29.
 *         在byte表示上比较TextPair的第一个变量
 */
public class E5_9 {

    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(E5_7.TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readInt(b2, s2);
            return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof E5_7.TextPair && b instanceof E5_7.TextPair) {
                return ((E5_7.TextPair) a).getFirst().compareTo(((E5_7.TextPair) b).getFirst());
            }
            return super.compare(a, b);
        }
    }
}
