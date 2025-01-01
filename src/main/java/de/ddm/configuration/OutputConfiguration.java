package de.ddm.configuration;

import lombok.Data;

@Data
public class OutputConfiguration {
	private final int inputReaderBatchSize = 10000;

	private final int dependencyMinerBatchSize = 100000;

	private final String outputFileName = "results.txt";
}
