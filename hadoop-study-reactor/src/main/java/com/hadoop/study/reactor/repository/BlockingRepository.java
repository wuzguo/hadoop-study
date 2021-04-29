package com.hadoop.study.reactor.repository;

public interface BlockingRepository<T> {

	void save(T value);

	T findFirst();

	Iterable<T> findAll();

	T findById(String id);
}
