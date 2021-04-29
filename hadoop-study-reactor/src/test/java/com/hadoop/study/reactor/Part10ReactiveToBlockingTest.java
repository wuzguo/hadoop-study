package com.hadoop.study.reactor;

import com.hadoop.study.reactor.domain.User;
import com.hadoop.study.reactor.repository.ReactiveRepository;
import com.hadoop.study.reactor.repository.ReactiveUserRepository;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;

/**
 * Learn how to turn Reactive API to blocking one.
 *
 * @author Sebastien Deleuze
 */
public class Part10ReactiveToBlockingTest {

	Part10ReactiveToBlocking reactor = new Part10ReactiveToBlocking();
	ReactiveRepository<User> repository = new ReactiveUserRepository();

//========================================================================================

	@Test
	public void mono() {
		Mono<User> mono = repository.findFirst();
		User user = reactor.monoToValue(mono);
		Assert.assertEquals(user, User.SKYLER);
	}

//========================================================================================

	@Test
	public void flux() {
		Flux<User> flux = repository.findAll();
		Iterable<User> users = reactor.fluxToValues(flux);
		Iterator<User> it = users.iterator();
		Assert.assertEquals(it.next(), User.SKYLER);
		Assert.assertEquals(it.next(), User.JESSE);
		Assert.assertEquals(it.next(), User.WALTER);
		Assert.assertEquals(it.next(), User.SAUL);
		Assert.assertFalse(it.hasNext());
	}

}
