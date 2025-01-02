package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.Reaper;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        List<Integer> batch; // Lombok will generate getColumnBatch()
        Map<Integer, HashSet<String>> batchValues;  // Important: Must match exact type from DependencyMiner
        int numColumns;
	}
	@Getter
	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = -4667745208356518160L;
	}


	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());
		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		int numColumns = message.getNumColumns();
		boolean[][] result = new boolean[numColumns][numColumns];
		// For each column in the batch
        for(Integer columnI : message.getBatch()) {
            Set<String> valuesI = message.getBatchValues().get(columnI);
            
            // Compare with all other columns
            for(int j = 0; j < numColumns; j++) {
                if(columnI == j) continue;
                
                Set<String> valuesJ = message.getBatchValues().get(j);
                if(valuesJ == null) continue; // Skip if column j hasn't been processed yet
                
                // Check if all non-null values in column i are contained in column j
                if(valuesI.stream().allMatch(valuesJ::contains)) {
                    result[columnI][j] = true; // Column i is included in column j
                }
            }
        }

		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result), message.getDependencyMinerLargeMessageProxy()));
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		//this.largeMessageProxy.tell(new LargeMessageProxy.ShutdownMessage());
		return Behaviors.stopped();
	}
}
