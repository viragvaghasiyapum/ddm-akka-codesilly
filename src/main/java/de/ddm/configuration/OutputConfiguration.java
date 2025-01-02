package de.ddm.configuration;

import lombok.Data;

@Data
public class OutputConfiguration {
	private final int inputBatchSize = 10000;
	private final String outputFileName = "results.txt";
}
