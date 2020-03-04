package com.torres.bubble;

public class Bubble01 {
    public static void main(String[] args) {
        int data[] = {8, 5, 9, 2, 3, 1, 7};
        bubbleSort02(data);
        System.out.println(java.util.Arrays.toString(data));
    }

    public static void bubbleSort01(int[] data) {
        int count = 0;
        for (int i = 0; i < data.length - 1; i++) {
            boolean flag = false;
            for (int j = 0; j < data.length - 1 - i; j++) {
                if (data[j] > data[j + 1]) {
                    count++;
                    flag = true;
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

    //Todo 冒泡排序法
    private static void bubbleSort02(int[] data) {
        for (int i = 0; i < data.length - 1; i++) {
            for (int j = 0; j < data.length - i - 1; j++) {
                if (data[j] > data[j + 1]) {
                    int temp = data[j];
                    data[j] = data[j + 1];
                    data[j + 1] = temp;
                }
            }
        }
    }


}
