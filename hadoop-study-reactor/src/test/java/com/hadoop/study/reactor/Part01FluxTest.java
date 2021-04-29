package com.hadoop.study.reactor;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Learn how to create Flux instances.
 *
 * @author Sebastien Deleuze
 * @see <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html">Flux Javadoc</a>
 */
public class Part01FluxTest {

	Part01Flux reactor = new Part01Flux();

//========================================================================================

	@Test
	public void empty() {
		Flux<String> flux = reactor.emptyFlux();

		StepVerifier.create(flux)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void fromValues() {
		Flux<String> flux = reactor.fooBarFluxFromValues();
		StepVerifier.create(flux)
				.expectNext("foo", "bar")
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void fromList() {
		Flux<String> flux = reactor.fooBarFluxFromList();
		StepVerifier.create(flux)
				.expectNext("foo", "bar")
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void error() {
		Flux<String> flux = reactor.errorFlux();
		StepVerifier.create(flux)
				.verifyError(IllegalStateException.class);
	}

//========================================================================================

	@Test
	public void countEach100ms() {
		Flux<Long> flux = reactor.counter();
		StepVerifier.create(flux)
				.expectNext(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
				.verifyComplete();
	}

}
