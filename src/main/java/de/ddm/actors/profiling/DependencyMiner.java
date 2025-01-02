package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.Reaper;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.OutputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.math.BigInteger;
import java.util.*;
import java.util.ArrayList;
import java.util.List;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		boolean[][] result;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();

		if (this.discoverNaryDependencies) {
			this.getContext().getLog().warn("Nary IND discovery not implemented, Initiating Unary IND discovery...");
		}
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.inputReadersList = new ArrayList<>(inputFiles.length);

		for (int id = 0; id < this.inputFiles.length; id++) {
			this.inputReadersList.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		}
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkersList = new ArrayList<>();
		this.hashMap = new HashMap<>();

		this.bitShifts = new int[this.inputFiles.length];
		this.isHeaderRead = new boolean[this.inputFiles.length];
		this.isInputReadingCompleted = new boolean[inputFiles.length];
		this.idleWorker = new ArrayList<>();
		this.workerProxy = new HashMap<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private final int batchSize = OutputConfigurationSingleton.get().getInputBatchSize();
	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkersList;
	private final String[][] headerLines;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<InputReader.Message>> inputReadersList;
	private final HashMap<String, BigInteger> hashMap;
	private final boolean[] isInputReadingCompleted;
	private final int[] bitShifts;
	private final boolean[] isHeaderRead;
	private int columnNumber = 0;
	private final HashMap<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxy;
	private final List<ActorRef<DependencyWorker.Message>> idleWorker;
	private boolean[][] result;
	private Iterator<BigInteger> valueStream;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReadersList) {
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		}
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		int id = message.getId();
		int shiftVal = message.getHeader().length;
		this.columnNumber += shiftVal;
		for(int i = id + 1; i < this.bitShifts.length; i++) {
			bitShifts[i] += shiftVal;
		}

		this.isHeaderRead[id] = true;
		this.getContext().getLog().info("Finished header reading of file : {}", id);
		for(boolean b : this.isHeaderRead) {
			if(!b) return this;
		}
		this.getContext().getLog().info("All header reading finished.");
		this.result = new boolean[this.columnNumber][this.columnNumber];
		for(int i = 0; i < this.columnNumber; i++) {
			for (int j = 0; j < this.columnNumber; j++) {
				this.result[i][j] = true;
			}
		}
		for (ActorRef<InputReader.Message> inputReader : this.inputReadersList) {
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		int fileId = message.getId();
		List<String[]> batch = message.getBatch();
		if(batch.isEmpty()) {
			this.isInputReadingCompleted[fileId] = true;
			for(boolean b : this.isInputReadingCompleted) {
				if(!b) {
					return this;
				}
			}
			this.valueStream = this.hashMap.values().iterator();
			this.getContext().getLog().info("Finished input reading after {} ms.", System.currentTimeMillis() - this.startTime);
			while(!this.idleWorker.isEmpty()) {
				if(!this.valueStream.hasNext()) {
					break;
				}
				ActorRef<DependencyWorker.Message> dependencyWorker = this.idleWorker.get(0);
				this.idleWorker.remove(dependencyWorker);
				this.dependencyWorkersList.add(dependencyWorker);
				this.forwardAvailableTasks(dependencyWorker);
			}
			return this;
		}

		for(String[] line : batch) {
			for(int i = 0; i < this.headerLines[fileId].length; i++){
				BigInteger representation = BigInteger.ONE.shiftLeft(this.bitShifts[fileId] + i);
				this.hashMap.merge(line[i], representation, (v1,v2) -> v1.or(v2));
			}
		}
		this.inputReadersList.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private void forwardAvailableTasks(ActorRef<DependencyWorker.Message> dependencyWorker){
		if(!this.valueStream.hasNext()) {
			return;
		}
		List<BigInteger> workerBatch =  new ArrayList<>();
		for(int i = 0; i < this.batchSize; i++) {
			if(!this.valueStream.hasNext()) {
				break;
			}
			workerBatch.add(this.valueStream.next());
		}
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(
				new DependencyWorker.TaskMessage(
						this.largeMessageProxy,
						workerBatch,
						this.columnNumber
				),
				this.workerProxy.get(dependencyWorker)
		));
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> worker = message.getDependencyWorker();
		if (!this.dependencyWorkersList.contains(worker) && !this.idleWorker.contains(worker)) {
			this.workerProxy.put(worker, message.getLargeMessageProxy());
			if(this.valueStream == null || !this.valueStream.hasNext()) {
				this.idleWorker.add(worker);
			} else {
				this.forwardAvailableTasks(worker);
				this.dependencyWorkersList.add(worker);
			}
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		boolean[][] workerResult = message.getResult();
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		for(int i = 0; i < this.columnNumber; i++) {
			for(int j = 0; j < this.columnNumber; j++) {
				this.result[i][j] = this.result[i][j] && workerResult[i][j];
			}
		}
		if(!this.valueStream.hasNext()){
			this.dependencyWorkersList.remove(dependencyWorker);
			this.idleWorker.add(dependencyWorker);
			if(this.dependencyWorkersList.isEmpty()) {
				this.end();
			}
		} else {
			this.forwardAvailableTasks(dependencyWorker);
		}
		return this;
	}

	private void end() {
		List<InclusionDependency> dependencies = new ArrayList<>();
		for(int i = 0; i < this.columnNumber; i++){
			for(int j = 0; j < this.columnNumber; j++) {
				if(i == j) {
					continue;
				}
				if(!this.result[i][j]) {
					continue;
				}
				int fileIdI = this.findFileId(i);
				int fileIdJ = this.findFileId(j);
				int columnIdI = i - this.bitShifts[fileIdI];
				int columnIdJ = j - this.bitShifts[fileIdJ];
				dependencies.add(
						new InclusionDependency(
								this.inputFiles[fileIdI],
								new String[]{this.headerLines[fileIdI][columnIdI]},
								this.inputFiles[fileIdJ],
								new String[]{this.headerLines[fileIdJ][columnIdJ]}
						)
				);
			}
		}
		this.resultCollector.tell(new ResultCollector.ResultMessage(dependencies));
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining in {} ms!", discoveryTime);
	}

	private int findFileId(int columnId){
		for(int fileId = 0; fileId < this.inputFiles.length - 1; fileId++){
			if(this.bitShifts[fileId + 1] > columnId) {
				return fileId;
			}
		}
		return this.inputFiles.length - 1;
	}

	private Behavior<Message> handle(Terminated signal) {
		this.getContext().getLog().error("Termination Signal is not handled.");
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.resultCollector.tell(new ResultCollector.ShutdownMessage());
		for(ActorRef<InputReader.Message> inputReader:this.inputReadersList) {
			inputReader.tell(new InputReader.ShutdownMessage());
		}
		return Behaviors.stopped();
	}
}
