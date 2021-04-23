package com.hadoop.study.reactor;

import org.junit.FixMethodOrder;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/23 17:16
 */

@FixMethodOrder
public class TestFluxSink {

    @Test
    public void testFluxSink() throws InterruptedException {
        final Flux<Integer> flux = Flux.create(fluxSink -> {
            //NOTE sink:class reactor.core.publisher.FluxCreate$SerializedSink
            System.out.printf("sink: %s", fluxSink.getClass());
            while (true) {
                System.out.println("sink next");
                fluxSink.next(ThreadLocalRandom.current().nextInt());
            }
        }, FluxSink.OverflowStrategy.BUFFER);

        //NOTE flux:class reactor.core.publisher.FluxCreate,prefetch:-1
        System.out.printf("flux:%s, prefetch:%s", flux.getClass(), flux.getPrefetch());

        flux.subscribe(e -> {
            System.out.printf("subscribe:%s", e);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e1) {
                System.out.println(e1.getMessage());
            }
        });

        TimeUnit.MINUTES.sleep(20);
    }
}
