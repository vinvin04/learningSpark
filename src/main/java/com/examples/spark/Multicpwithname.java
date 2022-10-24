package com.examples.spark;

public class Multicpwithname {
    private volatile Integer count = 1;
    private volatile String threadIdToRun = "1";
    private Object object = new Object();

    public static void main(String[] args) {

        Multicpwithname testClass = new Multicpwithname();
        Thread t1 = new Thread(testClass.new Printer());
        t1.setName("1");
        Thread t2 = new Thread(testClass.new Printer());
        t2.setName("2");


        t1.start();
        t2.start();
    }

    class Printer implements Runnable {

        public Printer() {
//            super();
//            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                while (count <= 20) {
                    synchronized (object) {
                        if (! Thread.currentThread().getName().equals(threadIdToRun)) {
                            object.wait();
                        } else {
                            System.out.println("Thread " + Thread.currentThread().getName() + " printed " + count);
                            count += 1;

                            if (Thread.currentThread().getName().equals("1"))
                                threadIdToRun = "2";
                            else
                                threadIdToRun = "1";

                            object.notifyAll();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}