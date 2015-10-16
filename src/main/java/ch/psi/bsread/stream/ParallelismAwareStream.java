package ch.psi.bsread.stream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ParallelismAwareStream<T> implements Stream<T> {
   private Stream<T> wrapped;
   private boolean allowParallelism;

   public ParallelismAwareStream(Stream<T> wrapped, boolean allowParallelism) {
      this.wrapped = wrapped.sequential();
      this.allowParallelism = allowParallelism;
   }

   @Override
   public Stream<T> parallel() {
      if (this.allowParallelism) {
         return this.wrapped.parallel();
      } else {
         return this.wrapped;
      }
   }

   // # From here, all calls are just forwarded to wrapper #
   // ######################################################

   @Override
   public Iterator<T> iterator() {
      return this.wrapped.iterator();
   }

   @Override
   public Spliterator<T> spliterator() {
      return this.wrapped.spliterator();
   }

   @Override
   public boolean isParallel() {
      return this.wrapped.isParallel();
   }

   @Override
   public Stream<T> sequential() {
      return this.wrapped;
   }

   @Override
   public Stream<T> unordered() {
      return this.wrapped.unordered();
   }

   @Override
   public Stream<T> onClose(Runnable closeHandler) {
      return this.wrapped.onClose(closeHandler);
   }

   @Override
   public void close() {
      this.wrapped.close();
   }

   @Override
   public Stream<T> filter(Predicate<? super T> predicate) {
      return this.wrapped.filter(predicate);
   }

   @Override
   public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
      return this.wrapped.map(mapper);
   }

   @Override
   public IntStream mapToInt(ToIntFunction<? super T> mapper) {
      return this.wrapped.mapToInt(mapper);
   }

   @Override
   public LongStream mapToLong(ToLongFunction<? super T> mapper) {
      return this.wrapped.mapToLong(mapper);
   }

   @Override
   public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
      return this.wrapped.mapToDouble(mapper);
   }

   @Override
   public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
      return this.wrapped.flatMap(mapper);
   }

   @Override
   public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
      return this.wrapped.flatMapToInt(mapper);
   }

   @Override
   public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
      return this.wrapped.flatMapToLong(mapper);
   }

   @Override
   public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
      return this.wrapped.flatMapToDouble(mapper);
   }

   @Override
   public Stream<T> distinct() {
      return this.wrapped.distinct();
   }

   @Override
   public Stream<T> sorted() {
      return this.wrapped.sorted();
   }

   @Override
   public Stream<T> sorted(Comparator<? super T> comparator) {
      return this.wrapped.sorted(comparator);
   }

   @Override
   public Stream<T> peek(Consumer<? super T> action) {
      return this.wrapped.peek(action);
   }

   @Override
   public Stream<T> limit(long maxSize) {
      return this.wrapped.limit(maxSize);
   }

   @Override
   public Stream<T> skip(long n) {
      return this.wrapped.skip(n);
   }

   @Override
   public void forEach(Consumer<? super T> action) {
      this.wrapped.forEach(action);
   }

   @Override
   public void forEachOrdered(Consumer<? super T> action) {
      this.wrapped.forEachOrdered(action);
   }

   @Override
   public Object[] toArray() {
      return this.wrapped.toArray();
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return this.wrapped.toArray(generator);
   }

   @Override
   public T reduce(T identity, BinaryOperator<T> accumulator) {
      return this.wrapped.reduce(identity, accumulator);
   }

   @Override
   public Optional<T> reduce(BinaryOperator<T> accumulator) {
      return this.wrapped.reduce(accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
      return this.wrapped.reduce(identity, accumulator, combiner);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
      return this.wrapped.collect(supplier, accumulator, combiner);
   }

   @Override
   public <R, A> R collect(Collector<? super T, A, R> collector) {
      return this.wrapped.collect(collector);
   }

   @Override
   public Optional<T> min(Comparator<? super T> comparator) {
      return this.wrapped.min(comparator);
   }

   @Override
   public Optional<T> max(Comparator<? super T> comparator) {
      return this.wrapped.max(comparator);
   }

   @Override
   public long count() {
      return this.wrapped.count();
   }

   @Override
   public boolean anyMatch(Predicate<? super T> predicate) {
      return this.wrapped.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super T> predicate) {
      return this.wrapped.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super T> predicate) {
      return this.wrapped.noneMatch(predicate);
   }

   @Override
   public Optional<T> findFirst() {
      return this.wrapped.findFirst();
   }

   @Override
   public Optional<T> findAny() {
      return this.wrapped.findAny();
   }

}
