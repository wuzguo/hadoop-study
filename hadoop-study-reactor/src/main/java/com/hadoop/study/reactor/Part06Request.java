package com.hadoop.study.reactor;


import com.hadoop.study.reactor.domain.User;
import com.hadoop.study.reactor.repository.ReactiveRepository;
import com.hadoop.study.reactor.repository.ReactiveUserRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Learn how to control the demand.
 *
 * @author Sebastien Deleuze
 */
public class Part06Request {

    ReactiveRepository<User> repository = new ReactiveUserRepository();

//========================================================================================

    // TODO Create a StepVerifier that initially requests all values and expect 4 values to be received
    StepVerifier requestAllExpectFour(Flux<User> flux) {
        return StepVerifier.create(flux)
            .expectNextCount(4)
            .expectComplete(); // TO BE REMOVED
    }

//========================================================================================

    // TODO Create a StepVerifier that initially requests 1 value and expects User.SKYLER then requests another value and expects User.JESSE then stops verifying by cancelling the source
    StepVerifier requestOneExpectSkylerThenRequestOneExpectJesse(Flux<User> flux) {
        return StepVerifier.create(flux, 1)
            .expectNext(User.SKYLER)
            .thenRequest(1)
            .expectNext(User.JESSE)
            .thenCancel(); // TO BE REMOVED
    }

//========================================================================================

    // TODO Return a Flux with all users stored in the repository that prints automatically logs for all Reactive Streams signals
    Flux<User> fluxWithLog() {
        return repository.findAll().log();
    }

//========================================================================================

    // TODO Return a Flux with all users stored in the repository that prints "Starring:" on subscribe, "firstname lastname" for all values and "The end!" on complete
    Flux<User> fluxWithDoOnPrintln() {
        return repository.findAll().doOnSubscribe(s -> System.out.println("Starring:"))
            .doOnNext(user -> System.out.println(user.getFirstname() + " " + user.getLastname()))
            .doOnComplete(() -> System.out.println("The end!"));
    }

}
