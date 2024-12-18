/*
 * Copyright 2020-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.support;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * An {@link ItemStreamWriter} decorator with a synchronized
 * {@link SynchronizedItemStreamWriter#write write()} method.
 * <p>
 * This decorator is useful when using a non thread-safe item writer in a multi-threaded
 * step. Typical delegate examples are the
 * {@link org.springframework.batch.item.json.JsonFileItemWriter JsonFileItemWriter} and
 * {@link org.springframework.batch.item.xml.StaxEventItemWriter StaxEventItemWriter}.
 *
 * <p>
 * <strong>It should be noted that synchronizing writes might introduce some performance
 * degradation, so this decorator should be used wisely and only when necessary. For
 * example, using a {@link org.springframework.batch.item.file.FlatFileItemWriter
 * FlatFileItemWriter} in a multi-threaded step does NOT require synchronizing writes, so
 * using this decorator in such use case might be counter-productive.</strong>
 * </p>
 *
 * @author Dimitrios Liapis
 * @author Mahmoud Ben Hassine
 * @param <T> type of object being written
 */
public class SynchronizedItemStreamWriter<T> implements ItemStreamWriter<T>, InitializingBean {

	private static final Log logger = LogFactory.getLog(SynchronizedItemStreamWriter.class);

	private ItemStreamWriter<T> delegate;

	private final Lock lock = new ReentrantLock();

	/**
	 * Set the delegate {@link ItemStreamWriter}.
	 * @param delegate the delegate to set
	 */
	public void setDelegate(ItemStreamWriter<T> delegate) {
		logger.debug("setDelegate called by delegate: " + System.identityHashCode(delegate));
		this.delegate = delegate;
	}

	/**
	 * This method delegates to the {@code write} method of the {@code delegate}.
	 */
	@Override
	public void write(Chunk<? extends T> items) throws Exception {
		logger.info("write called for delegate " + System.identityHashCode(this.delegate) + ", try to get lock");
		this.lock.lock();
		logger.info("OK for the lock for delegate: " + System.identityHashCode(this.delegate));
		try {
			this.delegate.write(items);
			logger.debug("items written by delegate: " + System.identityHashCode(this.delegate));
		}
		finally {
			logger.debug("try to unlock for delegate: " + System.identityHashCode(this.delegate));
			this.lock.unlock();
			logger.debug("unlocked for delegate: " + System.identityHashCode(this.delegate));
		}
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		logger.debug("open called by delegate: " + System.identityHashCode(this.delegate) + " / context: " + System.identityHashCode(executionContext));
		this.delegate.open(executionContext);
		logger.debug("opened for delegate: " + System.identityHashCode(this.delegate));
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		logger.debug("update called by delegate: " + System.identityHashCode(this.delegate) + " / context: " + System.identityHashCode(executionContext));
		this.delegate.update(executionContext);
	}

	@Override
	public void close() throws ItemStreamException {
		logger.debug("close called by delegate: " + System.identityHashCode(this.delegate));
		this.delegate.close();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		logger.debug("afterPropertiesSet called by delegate: " + System.identityHashCode(this.delegate));
		Assert.state(this.delegate != null, "A delegate item writer is required");
	}

}
