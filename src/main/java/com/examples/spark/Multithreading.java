package com.examples.spark;

public class Multithreading implements Runnable {
    private static volatile Integer count = 1;
    private static volatile String threadId = "1";

    static Object object = new Object();
    @Override
    public void run() {
        try {
            while (count <= 10) {
                synchronized (object) {
                    if(!Thread.currentThread().getName().equals(threadId)) {
                        object.wait();
                    }
                    else {
                        System.out.println(Thread.currentThread().getName()+" "+ count);
                        count++;
                        if(threadId.equals("1"))
                            threadId = "2";
                        else threadId = "1";
                        object.notifyAll();
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Multithreading t1m = new Multithreading();
        Multithreading t2m = new Multithreading();
        Thread t1 = new Thread(t1m);
        Thread t2 = new Thread(t2m);

        t1.setName("1");
        t2.setName("2");
        t1.start();
        t2.start();
    }
}
