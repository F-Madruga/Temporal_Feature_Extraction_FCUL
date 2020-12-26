package doinparallel;

@FunctionalInterface
public interface ParallelizableWork<T> {
    T doIteration(int start, int end);
}
