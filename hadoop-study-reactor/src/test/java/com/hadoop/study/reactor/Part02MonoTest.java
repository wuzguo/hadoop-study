package com.hadoop.study.reactor;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Learn how to create Mono instances.
 *
 * @author Sebastien Deleuze
 * @see <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html">Mono Javadoc</a>
 */
public class Part02MonoTest {

	Part02Mono reactor = new Part02Mono();

//========================================================================================

	@Test
	public void empty() {
		Mono<String> mono = reactor.emptyMono();
		StepVerifier.create(mono)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void noSignal() {
		Mono<String> mono = reactor.monoWithNoSignal();
		StepVerifier
				.create(mono)
				.expectSubscription()
				.expectTimeout(Duration.ofSeconds(1))
				.verify();
	}

//========================================================================================

	@Test
	public void fromValue() {
		Mono<String> mono = reactor.fooMono();
		StepVerifier.create(mono)
				.expectNext("foo")
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void error() {
		Mono<String> mono = reactor.errorMono();
		StepVerifier.create(mono)
				.verifyError(IllegalStateException.class);
	}

}
