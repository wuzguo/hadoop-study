package com.hadoop.study.reactor;

import com.hadoop.study.reactor.domain.User;
import com.hadoop.study.reactor.repository.BlockingUserRepository;
import com.hadoop.study.reactor.repository.ReactiveRepository;
import com.hadoop.study.reactor.repository.ReactiveUserRepository;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Learn how to call blocking code from Reactive one with adapted concurrency strategy for
 * blocking code that produces or receives data.
 * <p>
 * For those who know RxJava:
 * - RxJava subscribeOn = Reactor subscribeOn
 * - RxJava observeOn = Reactor publishOn
 *
 * @author Sebastien Deleuze
 * @see Flux#subscribeOn(Scheduler)
 * @see Flux#publishOn(Scheduler)
 * @see Schedulers
 */
public class Part11BlockingToReactiveTest {

    Part11BlockingToReactive reactor = new Part11BlockingToReactive();

//========================================================================================

    @Test
    public void slowPublisherFastSubscriber() {
        BlockingUserRepository repository = new BlockingUserRepository();
        Flux<User> flux = reactor.blockingRepositoryToFlux(repository);
        assertThat(repository.getCallCount()).isEqualTo(0).withFailMessage("The call to findAll must be deferred until the flux is subscribed");
        StepVerifier.create(flux)
                .expectNext(User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
                .verifyComplete();
    }

//========================================================================================

    @Test
    public void fastPublisherSlowSubscriber() {
        ReactiveRepository<User> reactiveRepository = new ReactiveUserRepository();
        BlockingUserRepository blockingRepository = new BlockingUserRepository(new User[]{});
        Mono<Void> complete = reactor.fluxToBlockingRepository(reactiveRepository.findAll(), blockingRepository);
        assertThat(blockingRepository.getCallCount()).isEqualTo(0);
        StepVerifier.create(complete)
                .verifyComplete();
        Iterator<User> it = blockingRepository.findAll().iterator();
        assertThat(it.next()).isEqualTo(User.SKYLER);
        assertThat(it.next()).isEqualTo(User.JESSE);
        assertThat(it.next()).isEqualTo(User.WALTER);
        assertThat(it.next()).isEqualTo(User.SAUL);
        assertThat(it.hasNext()).isFalse();
    }

}
