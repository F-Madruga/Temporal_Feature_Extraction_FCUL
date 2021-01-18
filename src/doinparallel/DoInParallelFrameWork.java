package doinparallel;

import java.util.ArrayList;
import java.util.List;

public class DoInParallelFrameWork {
    public static <T> List<T> doInParallel(ParallelizableWork<T> pw, int nIterations) {
        int defaultNThreads = Runtime.getRuntime().availableProcessors();
        return doInParallel(pw, nIterations, defaultNThreads);
    }



    public static <T> List<T> doInParallel(ParallelizableWork<T> pw, int nIterations, int nthreads) {

        Thread[] threads = new Thread[nthreads];
        List<T> results = new ArrayList<T>();

        for (int ti = 0; ti < nthreads; ti++) {
            final int tid = ti;
            results.add(null);
            threads[tid] = new Thread( () -> {

                int start_i = tid * nIterations / nthreads; // 0 * M / T   | 1 * M / T  | 2 * M / T  |  3 * M  /T
                int end_i = (tid+1) * nIterations / nthreads;	 // 1 * M / T	| 2 * M / T  | 3 * M / T  |  4 * M / T
                if (tid == nthreads-1) {
                    end_i = nIterations;
                }

                results.set(tid, pw.doIteration(start_i, end_i));
            });
            threads[tid].start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                System.out.println("Thread " + t + " was interrupted");
            }
        }


        return results;
    }
}

