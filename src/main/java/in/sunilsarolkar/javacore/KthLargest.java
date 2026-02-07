package in.sunilsarolkar.javacore;

import java.io.*;
import java.util.*;
import java.util.stream.*;

public class KthLargest {
    public static void main(String[] args) {
        // Scanner is easier to remember than BufferedReader
        Scanner sc = new Scanner(System.in);

        // Check if input exists to avoid NoSuchElementException
        if (sc.hasNext()) {
            // Pattern 1: Single Integer N followed by an Array
             int n = sc.nextInt();
             int[] arr = new int[n];
             for(int i=0; i<n-1; i++) arr[i] = sc.nextInt();

             int k=sc.nextInt();

             (new KthLargest()).findKthLargest(arr,k);

            // Pattern 2: Reading raw lines (Common for String manipulation)
            // String line = sc.nextLine();
        }

        // Quick Print Logic
        // System.out.println(result);

        sc.close();
    }

    public int findKthLargest(int[] nums, int k) {
        Deque<Integer> arr=new ArrayDeque<Integer>(k);

        for(int n:nums){
            arr.add(n);
            while(arr.size()>k){
                arr.poll();
            }

            arr.add(n);
        }

        return arr.isEmpty()?arr.pollLast():-1;

    }
}