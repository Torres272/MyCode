package com.torres.quick;

public class QuickSort {
    public static void main(String[] args) {
        int data[] = {8, 5, 9, 2, 3, 1, 7};
        quickSort(data, 0, data.length - 1);
        System.out.println(java.util.Arrays.toString(data));
    }

    public static void quickSort(int[] data, int low, int high) {
        if (low < high) {
            int middle = getMiddle(data, low, high);
            quickSort(data, low, middle - 1);
            quickSort(data, middle + 1, high);
        }
    }

    public static int getMiddle(int[] data, int low, int high) {
        int temp = data[low];
        while (low < high) {
            while (low < high && temp <= data[high]) {
                high--;
            }
            data[low] = data[high];

            while (low < high && temp >= data[low]) {
                low++;
            }
            data[high] = data[low];
        }
        data[low] = temp;
        return low;
    }

    private static void quickSort01(int[] data, int low, int high) {
        if (low < high) {

        }
    }

    private static int getMiddle01(int[] data,int low,int high){
        return 0;
    }
}
