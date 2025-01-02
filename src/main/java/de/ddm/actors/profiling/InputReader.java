package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.actors.patterns.Reaper;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.OutputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputReader extends AbstractBehavior<InputReader.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadHeaderMessage implements Message {
		private static final long serialVersionUID = 1729062814525657711L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadBatchMessage implements Message {
		private static final long serialVersionUID = -7915854043207237318L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = -8935783987324578324L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());
		this.id = id;
		this.csvReader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = InputConfigurationSingleton.get().getHeader(inputFile);
		if (InputConfigurationSingleton.get().isFileHasHeader()) {
			this.csvReader.readNext();
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final int batchSize = OutputConfigurationSingleton.get().getInputBatchSize();
	private final CSVReader csvReader;
	private final String[] header;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
		return this;
	}

	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		List<String[]> batch = new ArrayList<>(this.batchSize);
		for (int i = 0; i < this.batchSize; i++) {
			String[] line = this.csvReader.readNext();
			if (line == null) {
				break;
			}
			batch.add(line);
		}
		message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, batch));
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.csvReader.close();
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) throws IOException {
		this.csvReader.close();
		return Behaviors.stopped();
	}
}
