package in.sunilsarolkar.javacore;

import java.util.*;

public class SlidingWIndowMax {

    public int[] maxSlidingWindow(int[] nums, int k) {
        Deque<Integer> deque=new ArrayDeque<>();
        int[] windowMax=new int[nums.length-k+1];
        int rsindex=0;
        for(int i=0;i<nums.length;i++){
            while(!deque.isEmpty() && deque.peekFirst()<(i-k))
                deque.pollFirst();

            while(!deque.isEmpty() && nums[deque.peekLast()]<nums[i])
                deque.pollLast();

            deque.offerLast(i);

            if(i>=k){
                windowMax[rsindex]=nums[deque.pollFirst()];
            }
        }

        return windowMax;
    }
}
