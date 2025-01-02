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
import de.ddm.utils.MemoryUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.math.BigInteger;
import java.util.*;

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
		if(this.discoverNaryDependencies)
			this.getContext().getLog().warn("Nary ING discovery is not yet implementeed in this program version. Starting unary ING discovery...");
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();
		this.hashMap = new HashMap<>();
		this.distinctValues = new HashMap<>();

		this.shifts = new int[this.inputFiles.length];
		this.headerReadDone = new boolean[this.inputFiles.length];
		this.inputReaderFinishedFlag = new boolean[inputFiles.length];
		this.idleDependencyWorker = new ArrayList<>();
		this.workerProxy = new HashMap<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private final int batchSize = OutputConfigurationSingleton.get().getInputReaderBatchSize();

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	private final HashMap<String, BigInteger> hashMap;
	private final HashMap<Integer, HashSet<String>> distinctValues;
	private final boolean[] inputReaderFinishedFlag;
	private final int[] shifts;
	private final boolean[] headerReadDone;
	private int numColumns = 0;

	private boolean[][] result;

	private Iterator<Integer> valueStream;

	private final List<ActorRef<DependencyWorker.Message>> idleDependencyWorker;

	private final HashMap<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxy;

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
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		//for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			//inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		int id = message.getId();
		int shift = message.getHeader().length;
		this.numColumns += shift;

		for(int i = id + 1; i < this.shifts.length; i++){
			shifts[i] += shift;
		}

		this.headerReadDone[id] = true;
		this.getContext().getLog().info("Finished header reading of File {}", id);
		for(boolean b : this.headerReadDone)
			if(!b) return this;

		this.getContext().getLog().info("Finished all header readings.");


		this.result = new boolean[this.numColumns][this.numColumns];
		for(int i = 0; i < this.numColumns; i++)
			for(int j = 0; j < this.numColumns; j++)
				this.result[i][j] = true;

		for (ActorRef<InputReader.Message> inputReader : this.inputReaders) {
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}

		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.

		int fileId = message.getId();
		List<String[]> batch = message.getBatch();

		if(batch.isEmpty()){
			this.inputReaderFinishedFlag[fileId] = true;
			for(boolean b : this.inputReaderFinishedFlag){
				if(!b)
					return this;
			}

			// Process all distinct values to create value sets for comparison
            for(Map.Entry<String, BigInteger> entry : this.hashMap.entrySet()) {
                if(entry.getKey() == null) continue; // Skip null entries
                BigInteger columnMask = entry.getValue();
                for(int i = 0; i < this.numColumns; i++) {
                    if(columnMask.testBit(i)) {
                        distinctValues.computeIfAbsent(i, k -> new HashSet<>()).add(entry.getKey());
                    }
                }
            }

			this.valueStream = this.distinctValues.keySet().iterator();

			this.getContext().getLog().info("Input Reading Finished after {}ms.", System.currentTimeMillis() - this.startTime);

			while(!this.idleDependencyWorker.isEmpty()){
				if(!this.valueStream.hasNext())
					break;
				ActorRef<DependencyWorker.Message> dependencyWorker = this.idleDependencyWorker.get(0);
				this.idleDependencyWorker.remove(dependencyWorker);
				this.dependencyWorkers.add(dependencyWorker);
				this.sendTaskIfAvailable(dependencyWorker);
			}
			return this;
		}

		for(String[] line : batch) {
            for(int i = 0; i < this.headerLines[fileId].length; i++) {
                if(line[i] != null && !line[i].trim().isEmpty()) {
                    BigInteger representation = BigInteger.ONE.shiftLeft(this.shifts[fileId] + i);
                    this.hashMap.merge(line[i], representation, BigInteger::or);
                }
            }
        }

		this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private void sendTaskIfAvailable(ActorRef<DependencyWorker.Message> dependencyWorker){
		if(!this.valueStream.hasNext())
			return;

		List<Integer> workerBatch =  new ArrayList<>();
        Map<Integer, HashSet<String>> batchValues = new HashMap<>();

		for(int i = 0; i < this.batchSize; i++) {
            if(!this.valueStream.hasNext())
                break;
            Integer columnId = this.valueStream.next();  // Now correctly gets Integer
            workerBatch.add(columnId);
            batchValues.put(columnId, this.distinctValues.get(columnId));
        }


		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(
            new DependencyWorker.TaskMessage(
                this.largeMessageProxy,
                workerBatch,
                batchValues,
                this.numColumns
            ),
            this.workerProxy.get(dependencyWorker)
        ));
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker) && !this.idleDependencyWorker.contains(dependencyWorker)) {
			this.workerProxy.put(dependencyWorker, message.getLargeMessageProxy());
			if(this.valueStream == null || !this.valueStream.hasNext()){
				this.idleDependencyWorker.add(dependencyWorker);
			}
			else{
				this.sendTaskIfAvailable(dependencyWorker);
				this.dependencyWorkers.add(dependencyWorker);
			}
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {

		boolean[][] workerResult = message.getResult();
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		for(int i = 0; i < this.numColumns; i++){
			for(int j = 0; j < this.numColumns; j++){
				this.result[i][j] = this.result[i][j] && workerResult[i][j];
			}
		}

		if(!this.valueStream.hasNext()){
			this.dependencyWorkers.remove(dependencyWorker);
			this.idleDependencyWorker.add(dependencyWorker);
			if(this.dependencyWorkers.size() == 0){
				this.end();
			}
		}
		else{
			this.sendTaskIfAvailable(dependencyWorker);
		}
		return this;
	}

	private void end() {
		List<InclusionDependency> dependencies = new ArrayList<>();
		for(int i = 0; i < this.numColumns; i++){
			for(int j = 0; j < this.numColumns; j++){
				if(i == j) continue;
				if(!this.result[i][j]) continue;
				int fileIdI = this.findFileId(i);
				int fileIdJ = this.findFileId(j);
				int columnIdI = i - this.shifts[fileIdI];
				int columnIdJ = j - this.shifts[fileIdJ];
				dependencies.add(new InclusionDependency(this.inputFiles[fileIdI], new String[]{this.headerLines[fileIdI][columnIdI]}, this.inputFiles[fileIdJ], new String[]{this.headerLines[fileIdJ][columnIdJ]}));
			}
		}
		this.resultCollector.tell(new ResultCollector.ResultMessage(dependencies));
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private int findFileId(int columnId){
		for(int fileId = 0; fileId < this.inputFiles.length - 1; fileId++){
			if(this.shifts[fileId + 1] > columnId)
				return fileId;
		}
		return this.inputFiles.length - 1;
	}

	private Behavior<Message> handle(Terminated signal) {
		this.getContext().getLog().error("Watched Worker terminated. This is unhandled.");
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message){
		//this.largeMessageProxy.tell(new LargeMessageProxy.ShutdownMessage());
		this.resultCollector.tell(new ResultCollector.ShutdownMessage());
		for(ActorRef<InputReader.Message> inputReader:this.inputReaders){
			inputReader.tell(new InputReader.ShutdownMessage());
		}
		return Behaviors.stopped();
	}
}
