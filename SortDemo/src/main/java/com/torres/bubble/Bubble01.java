package com.torres.bubble;

public class Bubble01 {
    public static void main(String[] args) {
        int data[] = {8,5,9,2,3,1,7};
        bubbleSort01(data);
        System.out.println(java.util.Arrays.toString(data));
    }

    public static void bubbleSort01(int[] data) {
        int count =0;
        for (int i = 0; i < data.length - 1; i++) {
            boolean flag = false;
            for (int j = 0; j < data.length - 1 - i; j++) {
                if (data[j] > data[j + 1]) {
                    count ++;
                    flag =true;
                    int tmp = data[j];
                    data[j] = data[j + 1];
                    data[j + 1] = tmp;
                }
            }
            if (!flag)
                break;
        }
        System.out.println(count);
    }
}
