package com.tripartite.flinkdemo;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class MainTest {
    Semaphore semaphore = new Semaphore(0, true);

    @Test
    public void testConnectKeyedByStream() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        semaphore.release();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                semaphore.acquire();
                System.out.println("waiting for 10 sec");
                Thread.sleep(10000);
                System.out.println();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                semaphore.release();
            }
            countDownLatch.countDown();
        });

        executorService.execute(() -> {
            try {
                semaphore.acquire();
                System.out.println("got the permit");
                System.out.println();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                semaphore.release();
            }
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

}