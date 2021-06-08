package com.amazonaws.ItemGenerator;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;

/**
 * Executes an asynchromous batch write to a DynamoDB table
 * 
 * @author rickhou
 *
 */
public class BatchLoad implements Runnable {
	private TableWriteItems items;

	/**
	 * Constructor
	 * 
	 * @param items - the collection of items to be written
	 */
	public BatchLoad(TableWriteItems items) {
		this.items = items;
		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	/**
	 * the runnable process to execute the batch write
	 */
	@Override
	public void run() {
		boolean run = true;
		try {
			while (run) {
				try {
					// execute the write and iterate if there are unprocessed items
					BatchWriteItemOutcome outcome = Main.db.batchWriteItem(items);
					while (outcome.getUnprocessedItems().size() > 0) {
						Thread.sleep(100);
						outcome = Main.db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
					}

					synchronized (Main.sync) {
						Main.numThreads.decrementAndGet();
					}

					run = false;
				} catch (InternalServerErrorException ex) {
					System.out.println("ERROR: Potential partition split detected.");
					Thread.sleep(1000);
				}
			}
		} catch (InterruptedException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}
}
