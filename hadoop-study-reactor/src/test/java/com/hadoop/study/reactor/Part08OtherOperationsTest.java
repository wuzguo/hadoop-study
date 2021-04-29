package com.hadoop.study.reactor;

import com.hadoop.study.reactor.domain.User;
import com.hadoop.study.reactor.repository.ReactiveRepository;
import com.hadoop.study.reactor.repository.ReactiveUserRepository;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import java.util.Arrays;
import java.util.List;

/**
 * Learn how to use various other operators.
 *
 * @author Sebastien Deleuze
 */
public class Part08OtherOperationsTest {

	Part08OtherOperations reactor = new Part08OtherOperations();

	final static User MARIE = new User("mschrader", "Marie", "Schrader");
	final static User MIKE = new User("mehrmantraut", "Mike", "Ehrmantraut");

//========================================================================================

	@Test
	public void zipFirstNameAndLastName() {
		Flux<String> usernameFlux = Flux.just(User.SKYLER.getUsername(), User.JESSE.getUsername(), User.WALTER.getUsername(), User.SAUL.getUsername());
		Flux<String> firstnameFlux = Flux.just(User.SKYLER.getFirstname(), User.JESSE.getFirstname(), User.WALTER.getFirstname(), User.SAUL.getFirstname());
		Flux<String> lastnameFlux = Flux.just(User.SKYLER.getLastname(), User.JESSE.getLastname(), User.WALTER.getLastname(), User.SAUL.getLastname());
		Flux<User> userFlux = reactor.userFluxFromStringFlux(usernameFlux, firstnameFlux, lastnameFlux);
		StepVerifier.create(userFlux)
				.expectNext(User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void fastestMono() {
		ReactiveRepository<User> repository = new ReactiveUserRepository(MARIE);
		ReactiveRepository<User> repositoryWithDelay = new ReactiveUserRepository(250, MIKE);
		Mono<User> mono = reactor.useFastestMono(repository.findFirst(), repositoryWithDelay.findFirst());
		StepVerifier.create(mono)
				.expectNext(MARIE)
				.verifyComplete();

		repository = new ReactiveUserRepository(250, MARIE);
		repositoryWithDelay = new ReactiveUserRepository(MIKE);
		mono = reactor.useFastestMono(repository.findFirst(), repositoryWithDelay.findFirst());
		StepVerifier.create(mono)
				.expectNext(MIKE)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void fastestFlux() {
		ReactiveRepository<User> repository = new ReactiveUserRepository(MARIE, MIKE);
		ReactiveRepository<User> repositoryWithDelay = new ReactiveUserRepository(250);
		Flux<User> flux = reactor.useFastestFlux(repository.findAll(), repositoryWithDelay.findAll());
		StepVerifier.create(flux)
				.expectNext(MARIE, MIKE)
				.verifyComplete();

		repository = new ReactiveUserRepository(250, MARIE, MIKE);
		repositoryWithDelay = new ReactiveUserRepository();
		flux = reactor.useFastestFlux(repository.findAll(), repositoryWithDelay.findAll());
		StepVerifier.create(flux)
				.expectNext(User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void complete() {
		ReactiveRepository<User> repository = new ReactiveUserRepository();
		PublisherProbe<User> probe = PublisherProbe.of(repository.findAll());
		Mono<Void> completion = reactor.fluxCompletion(probe.flux());
		StepVerifier.create(completion)
				.verifyComplete();
		probe.assertWasRequested();
	}

//========================================================================================

	@Test
	public void nullHandling() {
		Mono<User> mono = reactor.nullAwareUserToMono(User.SKYLER);
		StepVerifier.create(mono)
				.expectNext(User.SKYLER)
				.verifyComplete();
		mono = reactor.nullAwareUserToMono(null);
		StepVerifier.create(mono)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void emptyHandling() {
		Mono<User> mono = reactor.emptyToSkyler(Mono.just(User.WALTER));
		StepVerifier.create(mono)
				.expectNext(User.WALTER)
				.verifyComplete();
		mono = reactor.emptyToSkyler(Mono.empty());
		StepVerifier.create(mono)
				.expectNext(User.SKYLER)
				.verifyComplete();
	}

//========================================================================================

	@Test
	public void collect() {
		ReactiveRepository<User> repository = new ReactiveUserRepository();
		Mono<List<User>> collection = reactor.fluxCollection(repository.findAll());
		StepVerifier.create(collection)
				.expectNext(Arrays.asList(User.SKYLER, User.JESSE, User.WALTER, User.SAUL))
				.verifyComplete();
	}

}
